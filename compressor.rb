require 'zlib'
require 'json'

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
