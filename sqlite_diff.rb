#Gems
require 'sqlite3'
require 'mysql2'
require 'lzma'
require 'hashdiff'

#Builtins
require 'json'
require 'yaml'
require 'set'
require 'date'
require 'net/ftp'
require 'digest'

CONFIG = YAML.load_file("config.yml")
TABLE = CONFIG["table"]
PRIMARY_KEY = CONFIG["primary_key"]
COLUMNS = YAML.load_file("columns.yml")
COLUMN_LIST = COLUMNS.join(',')
BASE_QUERY = "select #{COLUMN_LIST} from #{TABLE}"
MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def md5(data)
  Digest::MD5.hexdigest(data)
end

def lzma_compress(path, delete_original = true)
  new_path = path + ".lzma"
  result = ""
  File.open(path, "rb") { |input| result = LZMA.compress(input.read) }
  File.open(new_path, "wb") { |output| output.write(result) }
  File.unlink(path) if delete_original
  [new_path, md5(result)]
end

def lzma_decompress(path, delete_original = true)
  new_path = path.gsub(/\.lzma\Z/, "")
  File.open(path, "rb") do |input|
    File.open(new_path, "wb") do |output|
      output.write(LZMA.decompress(input.read))
    end
  end
  File.unlink(path) if delete_original
  new_path
end

def get_new_master_data(start_date = nil)
  db = Mysql2::Client.new(YAML.load_file("remote_db.yml"))
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
  lzma_compress(path)
end

def write_compressed_json(path, obj)
  path = path + ".lzma"
  result = LZMA.compress(JSON.generate(obj))
  checksum = md5(result)
  File.open(path + ".lzma", "wb") { |file| file.write(result) }
  [path, checksum]
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
    if old_db_path.end_with?(".lzma")
      print "Decompressing existing database..."
      old_db_path = lzma_decompress(old_db_path, false)
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
    result = write_compressed_db(new_db_path, schema, existing_rows.values)
    puts " Done (#{result.first}, md5: #{result.last})"

    print "Writing differences to compressed json file..."
    output = {
      columns: COLUMNS,
      modified: modified_rows,
      added: added_rows.map(&:values),
      deleted: deleted_rows
    }
    result = write_compressed_json(diff_path, output)
    puts " Done (#{result.first}, md5: #{result.last})"
  else
    puts "No changes to upload"
  end
end

def diff_from_file(file)
  if file
    filename = File.basename(file).partition(".").first
    previous_time_string = filename.match(/-(\d{8}T\d{6})\Z/){ |match| match[1] }
    previous_time = DateTime.parse(previous_time_string) if previous_time_string
  else
    previous_time = nil
  end

  current_time = Time.now.utc.strftime("%Y%m%dT%H%M%S")
  prefix = CONFIG["output_prefix"]
  generate_diff(previous_time, file, "#{prefix}data-#{current_time}.db", "#{prefix}diff-#{current_time}.json")
end

diff_from_file(ARGV.first)
