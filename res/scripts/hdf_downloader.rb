###########################################################################
# Copyright (c) 2015 by Regents of the University of Minnesota.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which 
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
###########################################################################

require 'fileutils'

FilePattern = /<a href="([^"]+)">(.+)<\/a>\s*(\d+-\w+-\d+)\s+(\d+:\d+)\s+([\d\.]+[KMG]|-)/
$ParallelSize = 16

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

def downloadFiles(files_to_download, downloadPath, error_files)
  puts "Downloading #{files_to_download.size} files to '#{downloadPath}'"
  partitions = []
  $ParallelSize.times {|i| partitions << (files_to_download.size * i / $ParallelSize)}
  partitions << files_to_download.size
  download_threads = []
  $ParallelSize.times do |thread_id|
    first, last = partitions[thread_id, 2]
    download_threads << Thread.new(first, last) { |_first, _last|
      (_first..._last).each do |file_id|
        url_to_download = files_to_download[file_id]
        snapshot_date = File.basename(File.dirname(url_to_download))
        output_dir = File.join(downloadPath, snapshot_date)
        temp_download_file = File.join($TempDownloadPath, File.basename(url_to_download))
  
        system("curl -sf '#{url_to_download}' -o '#{temp_download_file}'")
        if $?.success?
          Dir.mkdir(output_dir) unless File.exists?(output_dir)
          if system("mv #{temp_download_file} #{output_dir}")
            puts "File #{url_to_download} downloaded successfully"
          else
            $stderr.puts "Error moving file #{downloadedFile}"
            error_files << url_to_download
          end
        else
          puts "Error downloading file #{url_to_download}"
          error_files << url_to_download
        end
      end # each file_id
    } # Thread
  end # $ParallelSize.times
  download_threads.each(&:join)
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

# Create temporary download path if not exists
$TempDownloadPath = File.join(downloadPath, 'tmp')
FileUtils.mkdir_p($TempDownloadPath) unless File.exists?($TempDownloadPath)

index_file = `curl -s '#{baseUrl}'`

all_files = []
index_file.scan(FilePattern) do |href|
  dir_name = File.basename($1)
  if dir_name =~ /^(\d\d\d\d)\.(\d\d)\.(\d\d)$/
    dir_date = Time.mktime($1.to_i, $2.to_i, $3.to_i)
    all_files << dir_name if date_range.nil? || (dir_date >= date_from && dir_date <= date_to)
  end
end

files_to_download = []
error_files = []

for snapshot_dir in all_files
  puts "Checking #{snapshot_dir}"
  snapshot_url = File.join(baseUrl, snapshot_dir)
  index_file = `curl -s '#{snapshot_url}/'`
  index_file.scan(FilePattern) do |href|
    cell_file_name = File.basename($1)
    if File.extname(cell_file_name).downcase == ".hdf"
      expected_download_file = File.join(downloadPath, snapshot_dir, cell_file_name)
      next if File.exists?(expected_download_file)
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
          if rectOverlap(query_range, [x1, y1, x2, y2])
            # Download this file
            files_to_download << "#{snapshot_url}/#{cell_file_name}"
          end
        end
      end
    end
  end
  
  if files_to_download.size >= $ParallelSize
    downloadFiles(files_to_download, downloadPath, error_files)
    files_to_download.clear
  end
end

# Download any remaining files
downloadFiles(files_to_download, downloadPath, error_files) unless files_to_download.empty?

# Give one last chance to error files
files_to_download = error_files
error_files = []
downloadFiles(files_to_download, downloadPath, error_files) unless files_to_download.empty?

$stderr.puts "Error downloading #{error_files.length} files" unless error_files.empty?

# Delete temporary download path
FileUtils.rm_rf($TempDownloadPath)