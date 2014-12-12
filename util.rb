require 'digest'

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
