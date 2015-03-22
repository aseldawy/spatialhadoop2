require 'rexml/document'

# Set the local Hadoop directory (defaults to current directory)
local_hadoop_home = File.expand_path(File.dirname(__FILE__))
hadoop1 = File.exists?(File.join(local_hadoop_home, 'conf'))
# Set remote Hadoop directory (defaults to scratch area)
remote_hadoop_home = "/export/scratch/#{ENV['USER']}/#{File.basename(local_hadoop_home)}"
remote_hadoop_home = local_hadoop_home
# Set remote linux username (defaults to current username)
remote_username = ENV['USER']

conf_dir = File.expand_path((hadoop1 ? "conf" : File.join("etc", "hadoop")), local_hadoop_home)

# Retrieve master and slave nodes from config files
if hadoop1
  masters_file = File.expand_path("masters", conf_dir)
  master = File.readlines(masters_file).map{|x| x.strip}.first
else
  core_conf = File.expand_path("core-site.xml", conf_dir)
  core_site = REXML::Document.new(File.read(core_conf))
  # Retrieve master name from the configuration file
  master_path = core_site.elements["//property[name='fs.default.name']/value"].text rescue nil
  if master_path && master_path =~ /hdfs:\/\/(.*):(\\d)*/
    master = $1
  end
end
slaves_file = File.expand_path("slaves", conf_dir)
slaves = File.readlines(slaves_file).map{|x| x.strip}

puts "Master: '#{master}'"
puts "Slaves: #{slaves.inspect}"

# Retrieve HDFS and log paths from config files
hdfs_conf = File.expand_path("hdfs-site.xml", conf_dir)
hdfs_site = REXML::Document.new(File.read(hdfs_conf))
hdfs_data_dir = hdfs_site.elements["//property[name='dfs.data.dir']/value"].text rescue nil
hdfs_name_dir = hdfs_site.elements["//property[name='dfs.name.dir']/value"].text rescue nil

mapred_conf = File.expand_path("mapred-site.xml", local_hadoop_home)
if File.exists?(mapred_conf)
  mapred_site = REXML::Document.new(File.read(mapred_conf))
  mapred_dir = mapred_site.elements["//property[name='mapred.local.dir']/value"].text rescue nil
end

log_dir = File.expand_path("logs", remote_hadoop_home)

# Add any additional machines you want to copy Hadoop to
machines = ([master] + slaves).uniq

# Retrieve specific tasks to perform from command line arguments
copy = !!ARGV.index('copy')
restart = !!ARGV.index('restart')
format = !!ARGV.index('format')
stop = !!ARGV.index('stop')
start = !!ARGV.index('start')
# Set default taks to copy if no tasks were provided
copy = true if !(copy || restart || format || start || stop)

ssh_opts = "-o StrictHostKeyChecking=no -x"

# Shutdown the cluster
if restart || stop
  puts "Shutting down the cluster"
  if hadoop1
    system("ssh #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/bin/stop-all.sh")
  else
    system("ssh #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/sbin/stop-yarn.sh")
    system("ssh #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/sbin/stop-dfs.sh")
  end
end

# Copy local Hadoop directory to all remote machines
if copy
  rsync_pattern = "rsync -e 'ssh -o ConnectTimeout=600 #{ssh_opts}' -a --copy-dirlinks --copy-links --delete --chmod=a+rX --exclude #{hdfs_data_dir} --exclude #{hdfs_name_dir} --exclude #{mapred_dir} #{log_dir} \#{from}/ \#{to}/"

  running_on_master = (master == `uname -n`.strip)
  puts(running_on_master ? "Running on master" : "Not running on master")
  # First, copy to the master machine, then copy from the master to all slaves
  unless running_on_master
    puts "Copying to the master node '#{master}'"
    puts "Creating directory"
    system("ssh -o ConnectTimeout=60 #{ssh_opts} #{remote_username}@#{master} mkdir -p #{remote_hadoop_home}")
    puts "Copying files"
    from = local_hadoop_home
    to = "#{remote_username}@#{master}:#{remote_hadoop_home}"
    rsync_command = rsync_pattern.sub('#{from}', from).sub('#{to}', to)
    system(rsync_command)
  end
  
  puts "Copying to slaves nodes"
  for machine in slaves
    next if machine == master # Skip in case master node is also a slave node (dual job)
    puts "Copying to slave '#{machine}'"
    puts "Creating directory"
    system("ssh -o ConnectTimeout=60 #{ssh_opts} #{remote_username}@#{machine} mkdir -p #{remote_hadoop_home}")
    puts "Copying files"
    from = remote_hadoop_home
    to = "#{remote_username}@#{machine}:#{remote_hadoop_home}"
    rsync_command = rsync_pattern.sub('#{from}', from).sub('#{to}', to)
    system("ssh -o ConnectTimeout=600 #{ssh_opts} #{remote_username}@#{master} \"#{rsync_command}\"")
  end
end

for machine in machines
  if restart
    puts "Delete old log files in '#{machine}'"
    system("ssh #{ssh_opts} #{remote_username}@#{machine} rm -rf #{log_dir}")
  end

  # Remove HDFS directory from remote machines
  if format
    puts "Creating HDFS directory in '#{machine}'"
    puts("ssh -o ConnectTimeout=60 #{ssh_opts} #{remote_username}@#{machine} mkdir -p #{hdfs_data_dir} #{hdfs_name_dir} #{mapred_dir}")
    system("ssh -o ConnectTimeout=60 #{ssh_opts} #{remote_username}@#{machine} mkdir -p #{hdfs_data_dir} #{hdfs_name_dir} #{mapred_dir}")
    system("ssh -o ConnectTimeout=300 #{ssh_opts} #{remote_username}@#{machine} rm -rf #{hdfs_data_dir} #{hdfs_name_dir} #{mapred_dir}")
  end
end

# Format HDFS in the master node
if format
  puts "Formatting HDFS"
  system("ssh -o ConnectTimeout=300 #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/bin/hadoop namenode -format")
end

# Start the cluster
if restart || start
  puts "Starting the cluster"
  if hadoop1
    system("ssh -o ConnectTimeout=300 #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/bin/start-all.sh")
  else
    system("ssh #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/sbin/start-dfs.sh")
    system("ssh #{ssh_opts} #{remote_username}@#{master} #{remote_hadoop_home}/sbin/start-yarn.sh")
  end
end

