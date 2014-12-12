#This script uses Bundler: http://bundler.io/

require 'rubygems'
require 'bundler/setup'

#Gems
require 'sqlite3'
require 'mysql2'
require 'hashdiff'
require 'active_support/core_ext/hash/indifferent_access'

#Builtins
require 'json'
require 'yaml'
require 'set'
require 'date'
require 'net/ftp'

#Local
require_relative "util"
require_relative "logger"
require_relative "compressor"
require_relative "ftp_session"

#TODO: Lockfile on FTP to prevent multiple instances running at once
#TODO: Check MD5s when downloading

CONFIG = YAML.load_file("config.yml").with_indifferent_access

class SQLiteDiff
  TABLE = CONFIG["table"]
  PRIMARY_KEY = CONFIG["primary_key"]
  COLUMNS = CONFIG["columns"]
  COLUMN_LIST = COLUMNS.join(',')
  BASE_QUERY = "select #{COLUMN_LIST} from #{TABLE}"
  MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
  SQLITE_DATETIME_FORMAT = "%s"

  def initialize
    @nesting_level = 0
    @compressor = Compressor.new
    @logger = Logger.new
    @ftp = FTPSession.new(CONFIG["ftp"].merge(compressor: @compressor, logger: @logger))
  end

  def main
    manifest = @logger.nest("Fetching manifest") { @ftp.manifest }
    most_recent = @logger.nest("Fetching most recent client database") { @ftp.most_recent_db(manifest) }
    new_files = @logger.nest("Calculating database differences") { diff_from_file(most_recent) }

    if new_files
      manifest += manifest + new_files.map{ |file| Util.manifest_data(file) }
      @logger.nest("Uploading database and diff to ftp") { new_files.each { |file| @ftp.put(file) } }
      @logger.nest("Writing updated manifest and checksum to ftp") { @ftp.manifest = manifest }
    else
      @logger.nest("Nothing to upload")
    end

    @logger.nest("Cleaning up") { clean_temp_files }
  end

private

  def get_new_master_data(start_date = nil)
    db = Mysql2::Client.new(CONFIG["database"])
    db.query("SET @@session.time_zone='+00:00'")
    changed_query = BASE_QUERY.dup
    changed_query << " where modified > \"#{start_date.strftime(MYSQL_DATETIME_FORMAT)}\"" if start_date

    schema = db.query("describe #{TABLE}").each_with_object({}) {|row, result| result[row["Field"]] = row["Type"]}
    primary_keys = Set.new(db.query("select #{PRIMARY_KEY} from #{TABLE}").map{ |row| row[PRIMARY_KEY] })
    changed_rows = db.query(changed_query)

    @logger.nest("Read #{changed_rows.count} changed rows of #{primary_keys.count} total")

    {
      schema: schema,
      primary_keys: primary_keys,
      changed_rows: changed_rows
    }
  end

  def write_compressed_db(path, schema, rows)
    db = SQLite3::Database.new(path)
    db.execute("drop table if exists #{TABLE}")

    schema_string = schema.map{ |name, type| "#{name} #{type}" }.join(",")
    db.execute("create table #{TABLE} (#{schema_string})")

    sub_group = "(#{(['?'] * schema.size).join(',')})"
    rows.each_slice(500) do |row_group|
      data = row_group.select{ |row| row[PRIMARY_KEY] }.map(&:values)
      subs = ([sub_group] * data.count).join(',')
      data.each { |row| row.map! { |item| item.respond_to?(:strftime) ? item.strftime(SQLITE_DATETIME_FORMAT) : item } }
      db.execute("insert into #{TABLE} (#{COLUMN_LIST}) values #{subs}", data)
    end
    db.close()
    result = @compressor.compress_file(path)
    @logger.nest(result)
    result
  end

  def calculate_diff_and_update_existing(master_pks, changed_rows, existing_rows)
    deleted_rows = []
    modified_rows = {}
    added_rows = []

    existing_rows.delete_if do |pk, row|
      if master_pks.include?(pk)
        false
      else
        deleted_rows << pk
        true
      end
    end

    changed_rows.each do |master_row|
      pk = master_row[PRIMARY_KEY]
      next unless pk
      existing_row = existing_rows[pk]
      if existing_row
        diffs = HashDiff.diff(existing_row, master_row)
        if diffs.count > 0
          result = diffs.each_with_object({}) do |diff, hash|
            raise "Nothing should be added or removed within a row" unless diff[0] == "~"
            hash[diff[1]] = diff[3]
          end
          modified_rows[pk] = result
        end
      else
        added_rows << master_row.values
      end

      existing_rows[pk] = master_row
    end

    @logger.nest("#{modified_rows.count} Modified, #{added_rows.count} Added, #{deleted_rows.count} Deleted")

    {
      columns: COLUMNS,
      modified: modified_rows,
      added: added_rows,
      deleted: deleted_rows
    }
  end

  def get_rows_from_sqlite(file)
    if file
      if file.end_with?(@compressor.extension)
        @logger.nest("Decompressing file")
        file = @compressor.decompress_file(file)
      end
      @logger.nest("Reading rows") do

        db = SQLite3::Database.new(file, results_as_hash: true)
        raw_rows = db.execute(BASE_QUERY).map{ |row| row.select{ |k, v| String === k } }.to_enum
        rows = raw_rows.each_with_object({}) do |row, result|
          pk = row[PRIMARY_KEY]
          result[pk] = row if pk
        end
        @logger.nest("Read #{rows.count} rows")
        rows
      end
    else
      @logger.nest("No database")
      {}
    end
  end

  def generate_diff(changed_since, old_db_path, new_db_path, diff_path)
    since_string = changed_since ? " changed since #{changed_since.strftime(MYSQL_DATETIME_FORMAT)}" : ""
    master_data = @logger.nest("Reading rows in master#{since_string}") do
      get_new_master_data(changed_since)
    end

    existing_rows = @logger.nest("Reading existing database") { get_rows_from_sqlite(old_db_path) }

    differences = @logger.nest("Calculating database differences") do
      calculate_diff_and_update_existing(master_data[:primary_keys], master_data[:changed_rows], existing_rows)
    end

    if (differences[:modified].count + differences[:added].count + differences[:deleted].count) > 0
      files = []

      files << @logger.nest("Writing changed database to compressed SQLite file") do
        schema = COLUMNS.each_with_object({}){ |column, result| result[column] = master_data[:schema][column] }
        write_compressed_db(new_db_path, schema, existing_rows.values)
      end

      files << @logger.nest("Writing differences to compressed json file") do
        @compressor.write_compressed_json(diff_path, differences)
      end

      files
    else
      @logger.nest("No changes")
      nil
    end
  end

  def clean_temp_files(base_dir = ".")
    %W(#{@compressor.extension.gsub('.', '')} db json md5).each do |ext|
      Dir["*.#{ext}"].each do |file|
        File.unlink(file)
        @logger.nest(file)
      end
    end
  end

  def diff_from_file(file)
    previous_time = file ? Util.timestamp_from_filename(file) : nil
    current_time = Time.now.utc.strftime("%Y%m%dT%H%M%S")
    prefix = CONFIG["output_prefix"]
    generate_diff(previous_time, file, "#{prefix}data-#{current_time}.db", "#{prefix}diff-#{current_time}.json")
  end

end

#FTPSession.new(CONFIG["ftp"].merge(compressor: Compressor.new, logger: Logger.new)).compress_and_upload_dir("tmp")

SQLiteDiff.new.main
