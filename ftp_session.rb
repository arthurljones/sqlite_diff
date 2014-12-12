require 'net/ftp'
require 'json'

require_relative "util"

class FTPSession
  def initialize(options)
    @compressor = options[:compressor]
    @logger = options[:logger]
    @session = Net::FTP.new(options[:server])
    @session.login(options[:username], options[:password])
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
