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
#TODO: Include file sizes in manifest so clients can decide smallest changeset to download
#TODO: Make sure everything flows correctly if there's no remote manifest (probbaly crashes right now)

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
  class << self
    def compress_data(data)
      StringIO.open do |stream|
        compress(stream, data)
        stream.string
      end
    end

    def decompress_data(data)
      StringIO.open do |stream|
        decompress(stream, data)
        stream.string
      end
    end

    def compress_file(path)
      new_path = path + extension
      data = File.read(path)
      File.open(new_path, "wb") { |file| compress(file, data) }
      new_path
    end

    def decompress_file(path)
      new_path = path.gsub(/#{Regexp.quote(extension)}\Z/, "")
      data = File.read(path)
      File.open(new_path, "wb") { |file| decompress(file, data) }
      new_path
    end

    def extension
      ".gz"
    end

    def write_compressed_json(path, obj)
      path = path + extension
      result = compress_data(JSON.generate(obj))
      File.open(path, "wb") { |file| file.write(result) }
      path
    end

  private

    def compress(stream, data)
      bz2 = Zlib::GzipWriter.new(stream)
      bz2.write(data)
      bz2.close
    end

    def decompress(stream)
      bz2 = ZLib::GzipReader.new(stream)
      result = bz2.read
      bz2.close
      result
    end
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
    compressed_data = @session.getbinaryfile("manifest.json#{@compressor.extension}", nil)
    data = @compressor.decompress_data(compressed_data)
    JSON.parse(data)
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
    .map{ |entry| entry[:file] }
    .select{ |filename| file =~ /data-/ }
    .max_by{ |filename| Util.timestamp_from_filename(file) }

    @session.getbinaryfile(most_recent_db)
    @logger.nest(most_recent)
    most_recent
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
    @compressor = Compressor
    @logger = Logger.new
    @ftp = FTPSession.new(@compressor, @logger)
  end

  def main
    @logger.nest("Connecting to FTP server")
    ftp = start_ftp_session
    @logger.nest(" Done")

    @logger.nest("Fetching manifest")
    manifest = get_manifest(ftp)
    @logger.nest(" Done")

    most_recent = @logger.nest("Fetching most recent client database") { get_most_recent_db(ftp, manifest) }
    new_files = @logger.nest("Calculating database differences") { diff_from_file(most_recent) }

    if new_files
      manifest += manifest + new_files.map{ |file| Util.manifest_data(file) }
      @logger.nest("Uploading database and diff to ftp") { new_files.each { |file| safe_put(ftp, file) } }
      @logger.nest("Writing updated manifest and checksum to ftp") { put_manifest(ftp, manifest) }
    else
      @logger.nest("Nothing to upload")
    end

    @logger.nest("Cleaning up") { clean_temp_files }

    ftp.close
  end


private


  def get_new_master_data(start_date = nil)
    db = Mysql2::Client.new(CONFIG["database"])
    changed_query = BASE_QUERY.dup
    changed_query << " where modified > \"#{start_date.strftime(MYSQL_DATETIME_FORMAT)}\"" if start_date

    schema = db.query("describe #{TABLE}").each_with_object({}) {|row, result| result[row["Field"]] = row["Type"]},
    primary_keys = Set.new(db.query("select #{PRIMARY_KEY} from #{TABLE}").map{ |row| row[PRIMARY_KEY] }),
    changed_rows = db.query(changed_query)

    @logger.nest("#{r[:changed_rows].count} of #{r[:primary_keys].count} changed")

    {
      schema: schema,
      primary_keys: primary_keys,
      changed_rows: changed_rows
    }
  end

  def get_existing_rows(path)
    existing = SQLite3::Database.new(path, results_as_hash: true)
    existing.execute(BASE_QUERY).map{ |row| row.select{ |k, v| String === k } }.to_enum
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
          result = diff.each_with_object({}) do |diff, hash|
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
    if old_db_path
      if old_db_path.end_with?(@compressor.extension)
        @logger.nest("Decompressing existing database")
        old_db_path = @compressor.decompress_file(old_db_path, false)
      end
      @logger.nest("Reading rows from local database")
      existing_rows = get_existing_rows(old_db_path).each_with_object({}) do |row, result|
        pk = row[PRIMARY_KEY]
        result[pk] = row if pk
      end
      @logger.nest(" Read #{existing_rows.count} rows")
    else
      @logger.nest("No existing database - starting fresh")
      existing_rows = {}
    end
  end

  def generate_diff(changed_since, old_db_path, new_db_path, diff_path)
    since_string = changed_since ? " changed since #{changed_since.strftime(MYSQL_DATETIME_FORMAT)}" : ""
    master_data = @logger.nest("Reading modified rows in master#{since_string}") do
      get_new_master_data(changed_since)
    end

    differences = @logger.nest("Calculating database differences") do
      calculate_diff_and_update_existing(master_data[:primary_keys], master_data[:changed_rows], existing_rows)
    end

    if (modified_rows.count + added_rows.count + deleted_rows.count) > 0
      files = []

      files << @logger.nest("Writing changed database to compressed SQLite file") do
        schema = COLUMNS.each_with_object({}){ |column, result| result[column] = master_data[:schema][column] }
        write_compressed_db(new_db_path, schema, existing_rows.values)
      end

      files << @logger.nest("Writing differences to compressed json file", -> (r) { "Done (#{r})" }) do
        write_compressed_json(diff_path, differences)
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

ftp = FTPSession.new(Compressor, Logger.new)
ftp.compress_and_upload_dir("tmp")
#differ.main
