# Set the local Hadoop directory (defaults to current directory)
local_hadoop_home = File.expand_path(File.dirname(__FILE__))
hadoop1 = File.exists?(File.join(local_hadoop_home, 'conf'))
# Set remote Hadoop directory (defaults to scratch area)
remote_hadoop_home = "/export/scratch/#{ENV['USER']}/#{File.basename(local_hadoop_home)}"
remote_hadoop_home = local_hadoop_home
# Set remote linux username (defaults to current username)
remote_username = ENV['USER']

conf_dir = File.expand_path((hadoop1 ? "conf" : File.join("etc", "hadoop")), local_hadoop_home)

def generate_jar_file
  # Create a custom class that extends Point and try to process using it
  temp_dir = "test_custom_class"
  Dir.mkdir(temp_dir) unless File.exists?(temp_dir)
  source_filename = File.join(temp_dir, "ConfigReader.java")
  File.open(source_filename, "w") do |f|
    f.puts <<-JAVA
public class ConfigReader {
  public static void main(String[] args) {
    for (String arg : args)
      System.out.println(new org.apache.hadoop.conf.Configuration().get(arg));
  }
}
    JAVA
  end
  
  # Compile the class
  required_jars = Dir.glob(File.join("**", "hadoop-core*jar"))
  required_jars = Dir.glob(File.join("**", "hadoop-common*jar"))
  puts "javac -cp #{required_jars.join(File::PATH_SEPARATOR)} #{source_filename}"
  system "javac -cp #{required_jars.join(File::PATH_SEPARATOR)} #{source_filename}"
  class_filename = source_filename.sub('.java', '.class')
  
  # Create the jar file
  jar_file = File.join(temp_dir, "custom_point.jar")
  system "jar cf #{jar_file} -C #{temp_dir} #{File.basename(class_filename)}"
  
  jar_file
end

config_reader_jar = generate_jar_file

# Retrieve master and slave nodes from config files
if hadoop1
  masters_file = File.expand_path("masters", conf_dir)
  master = File.readlines(masters_file).map{|x| x.strip}.first
else
  master_path = `bin/hadoop jar #{config_reader_jar} ConfigReader fs.default.name`
  if master_path && master_path =~ /hdfs:\/\/(.*):(\\d)*/
    master = $1
  end
end
slaves_file = File.expand_path("slaves", conf_dir)
slaves = File.readlines(slaves_file).map{|x| x.strip}

puts "Master: '#{master}'"
puts "Slaves: #{slaves.inspect}"

# Retrieve Temporary dirs from config files
temp_dirs = `bin/hadoop jar #{config_reader_jar} ConfigReader dfs.data.dir dfs.name.dir mapred.local.dir hadoop.tmp.dir`.split(/[\r\n]/)
temp_dirs.delete_if {|dir| dir == "null" || dir.length == 0}
puts "Temporary dirs #{temp_dirs.inspect}"

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
  rsync_pattern = "rsync -e 'ssh -o ConnectTimeout=600 #{ssh_opts}' -a --copy-dirlinks --copy-links --delete --chmod=a+rX \#{from}/ \#{to}/"

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
  # Remove temporary directories from remote machines
  if format
    puts "Removing temporary directories in '#{machine}'"
    system("ssh -o ConnectTimeout=60 #{ssh_opts} #{remote_username}@#{machine} mkdir -p #{temp_dirs.join ' '}")
    system("ssh -o ConnectTimeout=300 #{ssh_opts} #{remote_username}@#{machine} rm -rf #{temp_dirs.join ' '}")
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

