#This script uses Bundler: http://bundler.io/

require 'rubygems'
require 'bundler/setup'

#Gems
require 'sqlite3'
require 'mysql2'
require 'hashdiff'
require 'xz'

#Builtins
require 'json'
require 'yaml'
require 'set'
require 'date'
require 'net/ftp'
require 'digest'

#TODO: Lockfile on FTP to prevent multiple instances running at once
#TODO: Check MD5s when downloading
#TODO: Include file sizes in manifest so clients can decide smallest changeset to download

CONFIG = YAML.load_file("config.yml")
TABLE = CONFIG["table"]
PRIMARY_KEY = CONFIG["primary_key"]
COLUMNS = CONFIG["columns"]
COLUMN_LIST = COLUMNS.join(',')
BASE_QUERY = "select #{COLUMN_LIST} from #{TABLE}"
MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
FTP_CONFIG = CONFIG["ftp"]
COMPRESSED_EXT = ".xz"

def md5(data)
  Digest::MD5.hexdigest(data)
end

def compress_file(path, delete_original = true)
  new_path = path + COMPRESSED_EXT
  XZ.compress_file(path, new_path)
  File.unlink(path) if delete_original && path != new_path
  { new_path => md5(File.read(new_path)) }
end

def decompress_file(path, delete_original = true)
  new_path = path.gsub(/#{Regexp.quote(COMPRESSED_EXT)}\Z/, "")
  XZ.decompress_file(path, new_path)
  File.unlink(path) if delete_original && path != new_path
  new_path
end

def get_new_master_data(start_date = nil)
  db = Mysql2::Client.new(CONFIG["database"])
  changed_query = BASE_QUERY.dup
  changed_query << " where modified > \"#{start_date.strftime(MYSQL_DATETIME_FORMAT)}\"" if start_date
  {
    schema: db.query("describe #{TABLE}").each_with_object({}) {|row, result| result[row["Field"]] = row["Type"]},
    primary_keys: Set.new(db.query("select #{PRIMARY_KEY} from #{TABLE}").map{ |row| row[PRIMARY_KEY] }),
    changed_rows: db.query(changed_query)
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
  rows.each_slice(500) do |rows|
    data = rows.select{|r| r[PRIMARY_KEY]}.map(&:values)
    subs = ([sub_group] * data.count).join(',')
    db.execute("insert into #{TABLE} (#{COLUMN_LIST}) values #{subs}", data)
  end
  db.close()
  compress_file(path)
end

def write_compressed_json(path, obj)
  path = path + COMPRESSED_EXT
  result = XZ.compress(JSON.generate(obj))
  File.open(path, "wb") { |file| file.write(result) }
  { path => md5(result) }
end

def debug_print(str)
  #print str
end

def generate_diff(changed_since, old_db_path, new_db_path, diff_path)
  since_string = changed_since ? " since #{changed_since.strftime(MYSQL_DATETIME_FORMAT)}" : ""
  print "Reading modified rows in master database#{since_string}..."
  master_data = get_new_master_data(changed_since)
  changed_count = master_data[:changed_rows].count
  puts " #{changed_count} of #{master_data[:primary_keys].count} changed"

  if old_db_path
    if old_db_path.end_with?(COMPRESSED_EXT)
      print "Decompressing existing database..."
      old_db_path = decompress_file(old_db_path, false)
      puts " Done"
    end
    print "Reading rows from local database..."
    existing_rows = get_existing_rows(old_db_path).each_with_object({}) do |row, result|
      pk = row[PRIMARY_KEY]
      result[pk] = row if pk
    end
    puts " Read #{existing_rows.count} rows"
  else
    puts "No existing database - starting fresh"
    existing_rows = {}
  end

  print "Calculating database differences..."
  deleted_rows = []
  modified_rows = {}
  added_rows = []

  master_pks = master_data[:primary_keys]
  existing_rows.delete_if do |pk, row|
    if master_pks.include?(pk)
      false
    else
      deleted_rows << pk
      true
    end
  end

  master_data[:changed_rows].each do |master_row|
    pk = master_row[PRIMARY_KEY]
    next unless pk
    existing_row = existing_rows[pk]
    if existing_row
      diff = HashDiff.diff(existing_row, master_row)
      if diff.count > 0
        result = diff.each_with_object({}) do |diff, hash|
          raise "Nothing should be added or removed within a row" unless diff[0] == "~"
          hash[diff[1]] = diff[3]
        end
        modified_rows[pk] = result
      end
    else
      added_rows << master_row
    end

    existing_rows[pk] = master_row
  end
  puts " Done"

  puts "- #{modified_rows.count} Rows Modified"
  puts "- #{added_rows.count} Rows Added"
  puts "- #{deleted_rows.count} Rows Deleted"

  if (modified_rows.count + added_rows.count + deleted_rows.count) > 0
    print "Writing changed database to compressed SQLite file..."
    schema = COLUMNS.each_with_object({}){ |column, result| result[column] = master_data[:schema][column] }
    data = write_compressed_db(new_db_path, schema, existing_rows.values)
    puts " Done (#{data})"

    print "Writing differences to compressed json file..."
    output = {
      columns: COLUMNS,
      modified: modified_rows,
      added: added_rows.map(&:values),
      deleted: deleted_rows
    }
    diff = write_compressed_json(diff_path, output)
    puts " Done (#{diff})"

    data.merge(diff)
  else
    puts "No changes"
    nil
  end
end

def timestamp_from_filename(filename)
  basename = File.basename(filename).partition(".").first
  previous_time_string = basename.match(/-(\d{8}T\d{6})\Z/){ |match| match[1] }
  previous_time = DateTime.parse(previous_time_string) if previous_time_string
end

def diff_from_file(file)
  previous_time = file ? timestamp_from_filename(file) : nil
  current_time = Time.now.utc.strftime("%Y%m%dT%H%M%S")
  prefix = CONFIG["output_prefix"]
  generate_diff(previous_time, file, "#{prefix}data-#{current_time}.db", "#{prefix}diff-#{current_time}.json")
end

def start_ftp_session
  ftp = Net::FTP.new(FTP_CONFIG["server"])
  ftp.login(FTP_CONFIG["username"], FTP_CONFIG["password"])
  ftp
end

def get_manifest(ftp)
  compressed_manifest_data = ftp.getbinaryfile("manifest.json#{COMPRESSED_EXT}", nil)
  manifest_data = XZ.decompress(compressed_manifest_data)
  JSON.parse(manifest_data)
end

def put_manifest(ftp, manifest)
end

def get_most_recent_db(ftp, manifest)
  db_files = manifest.keys.select{|file, md5| file =~ /data-/}
  db_files_by_timestamp = db_files.each_with_object({}){|(file, md5), result| result[timestamp_from_filename(file)] = file}
  most_recent = db_files_by_timestamp[db_files_by_timestamp.keys.max]
  ftp.getbinaryfile(most_recent)

  most_recent
end

def main
  print "Connecting to FTP server..."
  ftp = start_ftp_session
  puts " Done"

  print "Fetching manifest..."
  manifest = get_manifest(ftp)
  puts " Done"

  print "Fetching most recent client database..."
  most_recent = get_most_recent_db(ftp, manifest)
  puts " Done (#{most_recent})"

  result = diff_from_file(most_recent)

  if result
    print "Uploading database and diff to ftp..."
    result.keys.each { |file| ftp.putbinaryfile(file) }
    puts " Done"

    print "Writing updated manifest and checksum to ftp..."
    manifest = manifest.merge(result)
    compressed_manifest, manifest_md5 = write_compressed_json("manifest.json", manifest).first
    ftp.putbinaryfile(compressed_manifest)
    manifest_checksum = "manifest.md5"
    File.open(manifest_checksum, "wb") { |file| file.write(manifest_md5) }
    ftp.putbinaryfile(manifest_checksum)
    puts " Done"
  else
    puts "Nothing to upload"
  end

  print "Cleaning up..."
  %W(#{COMPRESSED_EXT.gsub('.', '')} db json md5).each do |ext|
    Dir["*.#{ext}"].each do |file|
      File.unlink(file)
    end
  end
  puts " Done"
end

main
