###########################################################################
# Copyright (c) 2015 by Regents of the University of Minnesota.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which 
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
###########################################################################

require 'fileutils'
$ParallelSize = 16

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
        out_filename = File.join(downloadPath, File.basename(url_to_download))
        next if File.exists?(out_filename)
        temp_download_file = File.join($TempDownloadPath, File.basename(url_to_download))
  
        system("curl -sf #{url_to_download} -o #{temp_download_file}")
        if $?.success?
          Dir.mkdir(output_dir) unless File.exists?(downloadPath)
          begin
            FileUtils.mv(temp_download_file, downloadPath)
            puts "File #{url_to_download} downloaded successfully"
          rescue => e
            $stderr.puts "Error moving file #{temp_download_file}"
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

index_url = "http://data.gdeltproject.org/events/index.html"
base_url = File.dirname(index_url)

index = `curl -s #{index_url}`
zip_files = []

index.scan(/<A HREF=\"([^\"]*.zip)\">/i) do |href|
  zip_files << File.join(base_url, $1)
end

downloadPath = "."
# Create temporary download path if not exists
$TempDownloadPath = File.join(downloadPath, 'tmp')
FileUtils.mkdir_p($TempDownloadPath) unless File.exists?($TempDownloadPath)

error_files = []
downloadFiles(zip_files, downloadPath, error_files)

$stderr.puts "Failed to download #{error_files.length} files" if error_files.any?

# Delete temporary download path
FileUtils.rm_rf($TempDownloadPath)
