#!/usr/bin/ruby
$download_url = ARGV[0]

unless $download_url
  if Dir.glob('/home/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2*.jar').any?
    $hadoop_version = 2
    $download_url = 'http://spatialhadoop.cs.umn.edu/downloads/spatialhadoop-2.3-2.tar.gz'
  elsif Dir.glob('/home/hadoop/hadoop-core-1*.jar').any?
    $hadoop_version = 1
    $download_url = 'http://spatialhadoop.cs.umn.edu/downloads/spatialhadoop-2.3.tar.gz'
  else
    raise "Unsupported Hadoop version"
  end
end

system("wget -qO- #{$download_url} | tar -xvz -C /home/hadoop/")
