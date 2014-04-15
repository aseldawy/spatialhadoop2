FilePattern = /<a href="([^"]+)">(.+)<\/a>\s*(\d+-\w+-\d+)\s+(\d+:\d+)\s+([\d\.]+[KMG]|-)/

def printUsage
  $stderr.puts "#{File.basename(__FILE__)} <base URL> [download path] [rect:west,south,east,north] [time:yyyy.mm.dd..yyyy.mm.dd]"
  $stderr.puts "If download path is not specified, data is downloaded to current folder"
  $stderr.puts "rect parameter is used to limit downloaded files to those overlapping this area"
end

def rangeOverlap(r1, r2)
  r1.last > r2.first && r2.last > r1.first
end


def rectOverlap(rect1, rect2)
  rangeOverlap(rect1[0]...rect1[2], rect2[0]...rect2[2]) &&
    rangeOverlap(rect1[1]...rect1[3], rect2[1]...rect2[3])
end

# Retrieve rectangle and delete from list of parameters
rect = ARGV.find { |x| x.start_with?("rect:") }
if rect
  ARGV.delete(rect)
  if rect =~ /^rect:(.+),(.+),(.+),(.+)$/
    query_range = [$1.to_f, $2.to_f, $3.to_f, $4.to_f]
  else
    $stderr.puts "Rectangle format is incorrect '#{rect}'"
    printUsage
    exit(1)
  end
end

time = ARGV.find { |x| x.start_with?("time:") }
if time
  ARGV.delete(time)
  if time =~ /^time:(\d\d\d\d)\.(\d\d)\.(\d\d)\.\.(\d\d\d\d)\.(\d\d)\.(\d\d)$/
    date_from = Time.mktime($1.to_i, $2.to_i, $3.to_i)
    date_to = Time.mktime($4.to_i, $5.to_i, $6.to_i)
    date_range = date_from..date_to
  else
    $stderr.puts "Invalid date format '#{time}'"
    printUsage
    exit(1)
  end
end

if ARGV.empty?
  $stderr.puts "Input base URL not specified"
  printUsage
  exit(1)
end

baseUrl = ARGV.delete_at(0)
downloadPath = ARGV.delete_at(0) || "."

index_file = `wget -qO- '#{baseUrl}'`

all_files = []
index_file.scan(FilePattern) do |href|
  dir_name = File.basename($1)
  if dir_name =~ /^(\d\d\d\d)\.(\d\d)\.(\d\d)$/
    dir_date = Time.mktime($1.to_i, $2.to_i, $3.to_i)
    all_files << dir_name if date_range.nil? || date_range.member?(dir_date)
  end
end

files_to_download = []
batch_size = 4

for snapshot_dir in all_files
  snapshot_url = "#{baseUrl}/#{snapshot_dir}"
  index_file = `wget -qO- '#{snapshot_url}'`
  index_file.scan(FilePattern) do |href|
    cell_file_name = File.basename($1)
    if File.extname(cell_file_name).downcase == ".hdf"
      if query_range.nil?
        # No spatial filter
        files_to_download << "#{snapshot_url}/#{cell_file_name}"
      else
        # Extract cell name
        if cell_file_name =~ /h(\d\d)v(\d\d)/
          h, v = $1.to_i, $2.to_i
          # Calculate coordinates on MODIS Sinusoidal grid
          x1 = h * 10 - 180
          y2 = (18 - v) * 10 - 90
          x2 = x1 + 10
          y1 = y2 - 10
          # Convert to Latitude Longitude
          lon1 = x1 / Math::cos(y1 * Math::PI / 180)
          lon2 = x1 / Math::cos(y2 * Math::PI / 180)
          x1 = [lon1, lon2].min
          lon1 = x2 / Math::cos(y1 * Math::PI / 180)
          lon2 = x2 / Math::cos(y2 * Math::PI / 180)
          x2 = [lon1, lon2].max
          if h == 21 && v == 6
          puts "Checking overlap of #{query_range.inspect} with #{[x1, y1, x2, y2].inspect}"
          end
          if rectOverlap(query_range, [x1, y1, x2, y2])
            # Download this file
            files_to_download << "#{snapshot_url}/#{cell_file_name}"
          end
        end
      end
    end
  end
  
  if files_to_download.size >= batch_size
    puts "Downloading #{files_to_download.size} files to '#{downloadPath}'"
    download_threads = files_to_download.map do |url_to_download|
      snapshot_date = File.basename(File.dirname(url_to_download))
      output_dir = File.join(downloadPath, snapshot_date)
      Thread.new {`wget -q --base=#{baseUrl} '#{url_to_download}' '--directory-prefix=#{output_dir}' --no-host-directories --no-clobber --continue`}
    end
    download_threads.each(&:join)
    files_to_download.clear
  end
end
