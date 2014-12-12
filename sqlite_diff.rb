#This script uses Bundler: http://bundler.io/

require 'rubygems'
require 'bundler/setup'

#Gems
require 'sqlite3'
require 'mysql2'
require 'hashdiff'

#Builtins
require 'json'
require 'yaml'
require 'set'
require 'date'
require 'net/ftp'
require 'digest'
require 'zlib'

#TODO: Lockfile on FTP to prevent multiple instances running at once
#TODO: Check MD5s when downloading

CONFIG = YAML.load_file("config.yml")

class Util
  def self.timestamp_from_filename(filename)
    basename = File.basename(filename).partition(".").first
    previous_time_string = basename.match(/-(\d{8}T\d{6})\Z/){ |match| match[1] }
    DateTime.parse(previous_time_string) if previous_time_string
  end

  def self.manifest_data(file)
    {
      :file => file,
      :checksum => Digest::MD5.hexdigest(File.read(file)),
      :size => File.size(file)
    }
  end
end

class Logger
  def initialize
    @nesting_level = 0
  end

  def nest(message)
    prefix = " " * 2 * @nesting_level
    puts("#{prefix}#{message}")

    if block_given?
      @nesting_level += 1
      result = yield
      @nesting_level -= 1
      result
    end
  end
end

class Compressor
  def compress_file(path)
    new_path = path + extension
    data = File.read(path)
    File.open(new_path, "wb") do |file|
      writer = Zlib::GzipWriter.new(file)
      writer.write(data)
      writer.close
    end
    new_path
  end

  def decompress_file(path)
    new_path = path.gsub(/#{Regexp.quote(extension)}\Z/, "")
    input = File.open(path)
    reader = Zlib::GzipReader.new(input)
    data = reader.read
    reader.close
    File.open(new_path, "wb") { |file| file.write(data) }
    new_path
  end

  def extension
    ".gz"
  end

  def write_compressed_json(path, obj)
    File.open(path, "wb") { |file| file.write(JSON.generate(obj)) }
    compress_file(path)
  end

end

class FTPSession

  def initialize(compressor, logger)
    config = CONFIG["ftp"]
    @compressor = compressor
    @logger = logger
    @session = Net::FTP.new(config["server"])
    @session.login(config["username"], config["password"])
    @list = Set.new(@session.nlst)
  end

  def manifest
    compressed_manifest = "manifest.json#{@compressor.extension}"
    if @list.include?(compressed_manifest)
      @session.getbinaryfile(compressed_manifest)
      decompressed_manifest = @compressor.decompress_file(compressed_manifest)
      JSON.parse(File.read(decompressed_manifest))
    else
      []
    end
  end

  def manifest=(manifest)
    compressed_manifest = @compressor.write_compressed_json("manifest.json", manifest)
    put(compressed_manifest)
    @logger.nest(compressed_manifest)
    manifest_checksum = Util.manifest_data(compressed_manifest)[:checksum]
    manifest_checksum_file = "manifest.md5"
    File.open(manifest_checksum_file, "wb") { |file| file.write(manifest_checksum) }
    put(manifest_checksum_file)
    @logger.nest(manifest_checksum_file)
  end

  def most_recent_db(manifest)
    most_recent_db = manifest
      .map{ |entry| entry["file"] }
      .select{ |filename| filename =~ /data-/ }
      .max_by{ |filename| Util.timestamp_from_filename(filename) }

    if most_recent_db
      @session.getbinaryfile(most_recent_db)
      @logger.nest(most_recent_db)
      most_recent_db
    end
  end

  def put(file)
    move_old_file(file) if @list.include?(file)
    @logger.nest("Uploading #{file}")
    @session.putbinaryfile(file)
    @list << file
  end

  def compress_and_upload_dir(dir)
    old_dir = Dir.getwd
    Dir.chdir(dir)
    new_manifest = @logger.nest("Compressing and uploading files in #{dir}") do
      Dir["*.*"].map do |file|
        @logger.nest(file) do
          if file.end_with?(@compressor.extension)
            @logger.nest("Skipping")
            next
          end

          compressed = false
          if %w(md5 html).include?(File.extname(file))
            @logger.nest("Not compressing")
          else
            @logger.nest("Compressing")
            file = @compressor.compress_file(file)
            compressed = true
          end

          @logger.nest("Adding to manifest")
          entry = Util.manifest_data(file)

          put(file)

          if compressed
            @logger.nest("Removing #{file}")
            File.unlink(file)
          end

          entry
        end
      end.compact
    end

    @logger.nest("Uploading manifest") do
      self.manifest = new_manifest
    end

    @logger.nest("Removing manifest files")
    Dir["manifest*"].each{ |file| File.unlink(file) }

    Dir.chdir(old_dir)
  end

private

  def rename(old_name, new_name)
    @session.rename(old_name, new_name)
    @list.delete(old_name)
    @list << new_name
  end

  def move_old_file(file, depth = 0)
    new_name = "#{file}.#{depth}"
    old_name =  depth > 0 ? "#{file}.#{depth - 1}" : file

    if @list.include?(new_name)
      if depth >= CONFIG["max_previous_file_versions"]
        @logger.nest("Dropping oldest version #{new_name}")
        @session.delete(new_name)
      else
        move_old_file(file, depth + 1)
      end
    end

    @logger.nest("Backing up old version #{old_name} -> #{new_name}")
    rename(old_name, new_name)
  end
end

class SQLiteDiff
  TABLE = CONFIG["table"]
  PRIMARY_KEY = CONFIG["primary_key"]
  COLUMNS = CONFIG["columns"]
  COLUMN_LIST = COLUMNS.join(',')
  BASE_QUERY = "select #{COLUMN_LIST} from #{TABLE}"
  MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

  def initialize
    @nesting_level = 0
    @compressor = Compressor.new
    @logger = Logger.new
    @ftp = FTPSession.new(@compressor, @logger)
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
      data = row_group.select{|r| r[PRIMARY_KEY]}.map(&:values)
      subs = ([sub_group] * data.count).join(',')
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

#ftp = FTPSession.new(Compressor.new, Logger.new)
#ftp.compress_and_upload_dir("tmp")
SQLiteDiff.new.main
