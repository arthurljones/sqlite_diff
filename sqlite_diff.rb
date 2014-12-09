require 'sqlite3'
require 'mysql2'
require 'json'
require 'hashdiff'
#require 'awesome_print'
require 'lzma'
require 'yaml'

CONFIG = YAML.load_file("config.yml")
SCHEMA = YAML.load_file("schema.yml")
TABLE = CONFIG["table"]
PRIMARY_KEY = CONFIG["primary_key"]
COLUMN_LIST = SCHEMA.keys.join(',')
QUERY = "select #{COLUMN_LIST} from #{TABLE} order by #{PRIMARY_KEY}"
schema_string = SCHEMA.map{ |name, type| "#{name} #{type}" }.join(",")
CREATE_STATEMENT = "create table #{TABLE} (#{schema_string})"

def lzma_compress(path, delete_original = true)
  new_path = path + ".lzma"
  File.open(path, "rb") do |input|
    File.open(new_path, "wb") do |output|
      output.write(LZMA.compress(input.read))
    end
  end
  File.unlink(path) if delete_original
  new_path
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

def get_master_rows
  master = Mysql2::Client.new(YAML.load_file("remote_db.yml"))
  master.query(QUERY).to_a
end

def get_existing_rows(path)
  existing = SQLite3::Database.new(path, results_as_hash: true)
  existing.execute(QUERY).map{ |row| row.select{ |k, v| String === k } }.to_enum
end

def new_db_from_rows(path, master_rows)
  db = SQLite3::Database.new(path)
  db.execute("drop table if exists #{TABLE}")
  db.execute(CREATE_STATEMENT)

  master_rows.each_slice(500) do |rows|
    data = rows.select{|r| r[PRIMARY_KEY]}.map(&:values)
    sub_group = "(#{(['?'] * SCHEMA.size).join(',')})"
    subs = ([sub_group] * data.count).join(',')
    #puts subs
    db.execute("insert into #{TABLE} (#{COLUMN_LIST}) values #{subs}", data)
  end
  db.close()
  lzma_compress(path)
end

def write_diffs(path, diffs)
  json = JSON.generate(diffs.merge(SCHEMA: SCHEMA.keys))
  File.open(path + ".lzma", "wb") { |file| file.write(LZMA.compress(json)) }
end

def debug_print(str)
  #print str
end

def generate_diff(old_db_path, new_db_path, diff_path)

  print "Reading rows from remote master database..."
  master_rows = get_master_rows
  puts " Done"

  if old_db_path.end_with?(".lzma")
    print "Decompressing existing database..."
    old_db_path = lzma_decompress(old_db_path, false)
    puts " Done"
  end

  print "Reading rows from local database..."
  existing_rows = get_existing_rows(old_db_path)
  puts " Done"

  deleted_rows = []
  modified_rows = {}
  added_rows = []
  unchanged_rows = 0

  next_existing = true
  next_master = false
  existing_row = nil
  existing_pk = nil

  print "Calculating database differences..."
  master_rows.each do |master_row|
    master_pk = master_row[PRIMARY_KEY]
    #For each master row, loop on as many existing rows as necessary
    loop do
      #puts ""
      #puts "looping"
      if next_existing
        existing_row = existing_rows.next
        #puts "next_existing #{!!existing_row}"
        existing_pk = existing_row ? existing_row[PRIMARY_KEY] : nil
        next_existing = false
      end

      if master_row && !master_pk
        debug_print "0"
        next_master = true
      elsif existing_row && !existing_pk
        debug_print "0"
        next_existing = true
      elsif !existing_row || (master_pk && master_pk < existing_pk)
        debug_print "+"
        #This row has been added in master
        added_rows << master_row.values
        next_master = true
      elsif !master_row || (existing_pk && existing_pk < master_pk)
        #This row has been removed from master
        debug_print "-"
        deleted_rows << existing_pk
        next_existing = true
      else
        if existing_row != master_row
          debug_print "~"
          diff = HashDiff.diff(existing_row, master_row)
          result = diff.inject({}) do |hash, diff|
            raise "Nothing should be added or removed within a row" unless diff[0] == "~"
            hash[diff[1]] = diff[3]
            hash
          end
          modified_rows[master_pk] = result
        else
          debug_print "="
          unchanged_rows += 1
        end
        next_existing = true
        next_master = true
      end

      if next_master
        next_master = false
        break
      end
    end
  end

  #Any remaining rows in the existing database have been deleted in master
  loop do
    next_existing = existing_rows.next rescue nil
    break unless next_existing
    debug_print "-"
    pk = next_existing[PRIMARY_KEY]
    deleted_rows << pk if pk
  end
  puts " Done"

  puts "- #{modified_rows.count} Rows Modified"
  puts "- #{added_rows.count} Rows Added"
  puts "- #{deleted_rows.count} Rows Deleted"
  puts "- #{unchanged_rows} Rows Unchanged"

  if (modified_rows.count + added_rows.count + deleted_rows.count) > 0
    print "Writing all master rows to compressed local SQLite database #{new_db_path}.lzma..."
    new_db_from_rows(new_db_path, master_rows)
    puts" Done"

    print "Writing differences to compressed json file #{diff_path}.lzma..."
    write_diffs(diff_path, modified: modified_rows, added: added_rows, deleted: deleted_rows)
    puts " Done"
  else
    puts "No changes to upload"
  end
end

time = Time.now.utc.strftime("%Y%m%dT%H%M%S")
prefix = CONFIG["output_prefix"]
generate_diff(ARGV[0], "#{prefix}data-#{time}.db", "#{prefix}diff-#{time}.json")
