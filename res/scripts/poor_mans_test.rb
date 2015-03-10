###########################################################################
# Copyright (c) 2015 by Regents of the University of Minnesota.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which 
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
###########################################################################

# This script performs an integration test for SpatialHadoop operations.
# It is not meant to be a full test with high coverage, but it is just a small
# script that checks if something really bad happened in the code.
# The test scenario is as follows
# 1- Generate a small file
# 2- Perform several operations on that file and make sure that nothing crashes
# 3- For operations with multiple implementations, make sure that the results
#    from all implementations are the same.

def system_check(cmd)
  puts "=> '#{cmd}'"
  success = system cmd
  raise "Error running '#{cmd}'" unless success 
end

$shadoop_jar = Dir.glob("**/spatialhadoop-*.jar").find{ |f| f =~ /\d/ }
$shadoop_cmd = "hadoop jar #$shadoop_jar"
$hadoop_home = File.expand_path("..", File.dirname(`which hadoop`))

ExtraConfigParams = "-D dfs.block.size=#{1024*1024}"
$InMBR = "0,0,10000,10000"

def generate_file(prefix, shape)
  filename = "#{prefix}.#{shape}"
  system_check "#$shadoop_cmd generate #{ExtraConfigParams} shape:#{shape} '#{filename}' size:10.mb mbr:#$InMBR -overwrite"
  filename
end

def index_file(filename, sindex)
  shape = File.extname(filename)[1..-1]
  indexed_filename = filename.dup; indexed_filename[-File.extname(filename).length, 0] = ".#{sindex}"
  system_check "#$shadoop_cmd index #{ExtraConfigParams} #{filename} #{indexed_filename} shape:#{shape} sindex:#{sindex} -overwrite -no-local"
  indexed_filename
end

TOO_SMALL = 1E-6

def array_equal?(ar1, ar2)
  values1 = ar1.map{|x| x.split(/[\s,]+/)}.flatten
  values2 = ar2.map{|x| x.split(/[\s,]+/)}.flatten
  unless values1.size == values2.size
    $stderr.puts "Non-equal sizes #{values1.size} != #{values2.size}, #{ar1.size} != #{ar2.size}"
    return false
  end
  values1.each_with_index do |v1, index|
    x1 = v1.to_f
    x2 = values2[index].to_f
    if ((x2 - x1).abs / [x1, x2].min) > TOO_SMALL
      $stderr.puts "Error! Different values #{v1}, #{values2[index]}" 
      return false
    end
  end
  return true
end

def read_results(hdfs_path)
  # Read file status
  matches, file_status = `hadoop fs -ls '#{hdfs_path}'`.split(/[\r\n]/, 2)
  num_of_matches = $1.to_i if matches =~ /Found (\d+) items/
  if num_of_matches == 1
    # Matched one file. Read its contents
    files_to_read = [hdfs_path]
  else
    # A directory that contains multiple files
    files_to_read = file_status.split(/[\r\n]/).grep(/^[^d]/).grep(/\/[^_][^\/]+$/).map do |line|
      line.split.last
    end
  end
  return `hadoop fs -cat #{files_to_read.join(' ')}`.split(/[\r\n]/).sort
end

def range_query(input, output, query, extra_args)
  shape = File.extname(input)[1..-1]
  system_check "#$shadoop_cmd rangequery #{ExtraConfigParams} #{input} #{output} shape:#{shape} rect:#{query} #{extra_args} -overwrite"
end

def test_range_query
  queries = ['40,990,1000,8000', $InMBR]
  %w(point rect).each do |shape|
    # Try range query with heap files
    heap_file = generate_file('test', shape)
    heap_file_count = `hadoop fs -cat #{heap_file}/data* | wc -l`.to_i
    for query in queries
      range_query(heap_file, 'results_heap_mr', query, '-no-local')
      results_heap_mr = read_results('results_heap_mr')
      
      # Try with indexed files
      %w(grid rtree r+tree str str+).each do |sindex|
        indexed_file = index_file(heap_file, sindex)
        
        # Make sure the indexed file has the same number of records as the heap file
        # Exclude R-tree and R+-tree as they use a binary format that cannot be easily determined
        unless %w(r+tree rtree).include?(sindex)
          indexed_file_count = read_results(indexed_file).uniq.size
          raise "#{sindex} index size #{indexed_file_count} should be equal to heap file size #{heap_file_count}" unless heap_file_count == indexed_file_count
        end
        
        # Run range query on the heap file and make sure it gives the same result as before
        range_query(indexed_file, "results_#{sindex}_mr", query, '-no-local')
        results_indexed_mr = read_results("results_#{sindex}_mr")
        raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_mr, results_heap_mr)
        
        range_query(indexed_file, "results_#{sindex}_local", query, '-local')
        results_indexed_mr = read_results("results_#{sindex}_local")
        raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_mr, results_heap_mr)

      end
    end
  end
end

def test_dup_avoidance_in_range_query
  shape = "rect"
  heap_file = generate_file('test', shape)
  
  # Build a grid index
  sindex = "grid"
  grid_file = index_file(heap_file, sindex)
  
  # Read global index
  gindex = `#$shadoop_cmd readfile #{grid_file}`
  gindex = gindex.lines.grep(/\((.*)\)-\((.*)\)/).map do |line|
    line=~ /\((.*)\)-\((.*)\)/
    x1, y1 = $1.split(',').map(&:to_f)
    x2, y2 = $2.split(',').map(&:to_f)
    [x1, y1, x2, y2]
  end
  
  gindex = gindex.sort_by{|cell| cell[0]}
  file_mbr = gindex.inject do |mbr, cell| 
    [
      [mbr[0], cell[0]].min,
      [mbr[1], cell[1]].min,
      [mbr[2], cell[2]].max,
      [mbr[3], cell[3]].max,
    ]
  end
  selected_cell = gindex.last
  query = "#{selected_cell[0]},#{file_mbr[1]},#{file_mbr[2]},#{file_mbr[3]}"
  
  range_query(heap_file, 'results_heap', query, '-no-local')
  results_heap = read_results('results_heap')
  range_query(grid_file, 'results_grid', query, '-no-local')
  results_grid = read_results('results_grid')
  
  raise "Error with duplicate avoidance" unless array_equal?(results_grid, results_heap)
end

def knn_query(input, output, point, k, extra_args="")
  shape = File.extname(input)[1..-1]
  system_check "#$shadoop_cmd knn #{ExtraConfigParams} #{input} #{output} shape:#{shape} point:#{point} k:#{k} #{extra_args} -overwrite"
end

def test_knn
  shape = 'point'
  k = 1000
  point = "700,232"

  # Try with heap files
  heap_file = generate_file('test', shape)
  knn_query(heap_file, 'knn_heap_local', point, k, '-local')
  results_heap_local = read_results 'knn_heap_local'
  knn_query(heap_file, 'knn_heap_mr', point, k, '-no-local')
  results_heap_mr = read_results 'knn_heap_mr'
  raise "Results of range query with local and MapReduce implementations differ" unless array_equal?(results_heap_local, results_heap_mr)
  
  # Try with indexed files
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file = index_file(heap_file, sindex)
    
    # Run knn on the heap file and make sure it gives the same result as before
    knn_query(indexed_file, "results_#{sindex}_local", point, k, '-local')
    results_indexed_local = read_results "results_#{sindex}_local"
    raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_local, results_heap_local)
      
    knn_query(indexed_file, "results_#{sindex}_mr", point, k, '-no-local')
    results_indexed_mr = read_results "results_#{sindex}_mr"
    raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_mr, results_heap_local) 
  end
end

def spatial_join(method, file1, file2, output, extra_args = '')
  shape = File.extname(file1)[1..-1]
  system_check "#$shadoop_cmd #{method} #{ExtraConfigParams} #{file1} #{file2} #{output} shape:#{shape} #{extra_args} -overwrite"
end

def test_spatial_join
  # Try with heap files
  heap_file1 = generate_file('test1', 'rect')
  heap_file2 = generate_file('test2', 'rect')

  spatial_join('sjmr', heap_file1, heap_file2, 'sjmr_heap')
  sjmr_heap_results = read_results "sjmr_heap"
  spatial_join('dj', heap_file1, heap_file2, 'bnlj')
  bnlj_results = read_results "bnlj"
  raise "Results of SJMR and BNLJ on heap files do not match" unless array_equal?(sjmr_heap_results, bnlj_results)

  # Try with indexes (same index for both files)
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file1 = index_file(heap_file1, sindex)
    indexed_file2 = index_file(heap_file2, sindex)

    # Run both SJMR and DJ on indexed files and check the result
    spatial_join('sjmr', indexed_file1, indexed_file2, "sjmr_#{sindex}")
    sjmr_indexed_results = read_results "sjmr_#{sindex}"
    raise "SJMR results with #{sindex} file does not match the heap file" unless array_equal?(sjmr_indexed_results, sjmr_heap_results) 
    
    spatial_join('dj', indexed_file1, indexed_file2, "dj_#{sindex}")
    dj_indexed_results = read_results "dj_#{sindex}"
    raise "Distributed Join results with #{sindex} file does not match the heap file" unless array_equal?(dj_indexed_results, bnlj_results)

    # Try one indexed file and one non-indexed file
    spatial_join('sjmr', indexed_file1, heap_file2, "sjmr_#{sindex}_heap")
    sjmr_one_side_results = read_results "sjmr_#{sindex}_heap"
    raise "SJMR results with one heap file and one #{sindex} file" unless array_equal?(sjmr_one_side_results, sjmr_heap_results)
    
    spatial_join('dj', indexed_file1, heap_file2, "dj_#{sindex}_heap")
    dj_one_side_results = read_results "dj_#{sindex}_heap"
    raise "DJ results with one heap file and one #{sindex} file" unless array_equal?(dj_one_side_results, bnlj_results)

    # Try one indexed file with a heap file (direct file not a directory struct)
    heap_file_name = `hadoop fs -ls #{heap_file2}`.lines.grep(/data/).first.split.grep(/data/).first
    spatial_join('dj', indexed_file1, heap_file_name, "dj_#{sindex}_heap")
    dj_one_side_results = read_results "dj_#{sindex}_heap"
    raise "DJ results with one heap file and one #{sindex} file" unless array_equal?(dj_one_side_results, sjmr_heap_results)
  end
end

def test_custom_class
  # Create a custom class that extends Point and try to process using it
  temp_dir = "test_custom_class"
  Dir.mkdir(temp_dir) unless File.exists?(temp_dir)
  source_filename = File.join(temp_dir, "CustomPoint.java")
  File.open(source_filename, "w") do |f|
    f.puts <<-JAVA
public class CustomPoint extends edu.umn.cs.spatialHadoop.core.Point {
  public static void main(String[] args) throws java.io.IOException, InterruptedException {
    edu.umn.cs.spatialHadoop.operations.FileMBR.main(args);
  }
}
    JAVA
  end
  
  # Compile the class
  required_jars = []
  required_jars += Dir.glob(File.join($hadoop_home, "lib", "spatialhadoop*.jar"))
  required_jars += Dir.glob(File.join($hadoop_home, "hadoop-core*jar"))
  required_jars << $shadoop_jar
  system_check "javac -cp #{required_jars.join(File::PATH_SEPARATOR)} #{source_filename}"
  class_filename = source_filename.sub('.java', '.class')
  
  # Create the jar file
  jar_file = File.join(temp_dir, "custom_point.jar")
  system_check "jar cf #{jar_file} -C #{temp_dir} #{File.basename(class_filename)}"
  
  # Create a test file with points and try with the custom jar file
  test_file = generate_file('test', 'point')

  # Test running a standard operation with the custom class
  system_check "#$shadoop_cmd mbr -libjars #{jar_file} #{test_file} shape:CustomPoint -no-local"
  system_check "#$shadoop_cmd mbr -libjars #{jar_file} #{test_file} shape:CustomPoint -local"

  # Test running the custom main method
  #system_check "hadoop jar #{jar_file} CustomPoint -libjars #$shadoop_jar #{test_file}/data_00001 shape:CustomPoint -no-local"
  #system_check "hadoop jar #{jar_file} CustomPoint -libjars #$shadoop_jar #{test_file}/data_00001 shape:CustomPoint -local"
end

def plot(input, output, extra_args="")
  shape = File.extname(input)[1..-1]
  system_check "#$shadoop_cmd gplot #{ExtraConfigParams} #{input} #{output} shape:#{shape} #{extra_args} -overwrite"
end

def test_plot
  shape = 'rect'
  rect = "100,100,8000,9000"
  
  # Try with heap files
  heap_file = generate_file('test', shape)
  plot(heap_file, 'heap_local.png', '-local')
  plot(heap_file, 'heap_mr.png', '-no-local')
  
  # Try with indexed files
  %w(grid rtree str+).each do |sindex|
    indexed_file = index_file(heap_file, sindex)
    # Single level plot
    plot(indexed_file, 'indexed_local.png', '-local')
    plot(indexed_file, 'indexed_mr.png', '-no-local')

    # Multilevel plot
    plot(indexed_file, 'indexed_local.png', '-local -pyramid')
    plot(indexed_file, 'indexed_mr.png', '-no-local -pyramid')
  end
  
end

# Main
if $0 == __FILE__
  test_range_query
  test_dup_avoidance_in_range_query
  test_knn
  test_spatial_join
  test_custom_class
  test_plot
end
