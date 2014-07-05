# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
# NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# This script performs an integration test for SpatialHadoop operations.
# It is not meant to be a full test with high coverage, but it is just a small
# script that checks if something really bad happened in the code.
# The test scenario is as follows
# 1- Generate a small file
# 2- Perform several operations on that file and make sure that nothing crashes
# 3- For operations with multiple implementations, make sure that the results
#    from all implementations are the same.

def system_check(cmd)
  success = system cmd
  raise "Error running '#{cmd}'" unless success 
end

ExtraConfigParams = "-D dfs.block.size=#{64*1024}"

def generate_file(prefix, shape)
  filename = "#{prefix}.#{shape}"
  system_check "shadoop generate #{ExtraConfigParams} shape:#{shape} '#{filename}' size:200.kb mbr:0,0,10000,10000 -overwrite"
  filename
end

def index_file(filename, sindex)
  shape = File.extname(filename)[1..-1]
  indexed_filename = filename.dup; indexed_filename[-File.extname(filename).length, 0] = ".#{sindex}"
  system_check "shadoop index #{ExtraConfigParams} #{filename} #{indexed_filename} shape:#{shape} sindex:#{sindex} -overwrite"
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

def range_query(input, output, query, extra_args)
  shape = File.extname(input)[1..-1]
  system_check "shadoop rangequery #{ExtraConfigParams} #{input} #{output} shape:#{shape} rect:#{query} #{extra_args} -overwrite"
end

def test_range_query
  %w(point rect).each do |shape|
    # Try range query with heap files
    heap_file = generate_file('test', shape)
    heap_file_count = `hadoop fs -cat #{heap_file}/data* | wc -l`.to_i
    query = '40,990,1000,8000'
    range_query(heap_file, 'results_mr', query, '-no-local')
    results_heap_mr = `hadoop fs -cat results_mr/part* | sort`.lines.to_a
    range_query(heap_file, 'results_local', query, '-local')
    results_heap_local = `hadoop fs -cat results_local | sort`.lines.to_a
    raise "Results of local and MapReduce implementations are different" unless array_equal?(results_heap_local, results_heap_mr)
    
    # Try with indexed files
    %w(grid rtree r+tree str str+).each do |sindex|
      indexed_file = index_file(heap_file, sindex)
      
      # Make sure the indexed file has the same number of records as the heap file
      # Exclude R-tree and R+-tree as they use a binary format that cannot be easily determined
      unless %w(r+tree rtree).include?(sindex)
        replicated = %w(grid r+tree str+).include?(sindex)
        indexed_file_count = (replicated && shape == 'rect') ? `hadoop fs -cat #{indexed_file}/part* | sort | uniq | wc -l`.to_i :
             `hadoop fs -cat #{indexed_file}/part* | wc -l`.to_i
        raise "#{sindex} index size #{indexed_file_count} should be equal to heap file size #{heap_file_count}" unless heap_file_count == indexed_file_count
      end
      
      # Run range query on the heap file and make sure it gives the same result as before
      range_query(indexed_file, "results_#{sindex}_mr", query, '-no-local')
      results_indexed_local = `hadoop fs -cat results_#{sindex}_mr/part* | sort`.lines.to_a
      raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_local, results_heap_local)
        
      range_query(indexed_file, "results_#{sindex}_local", query, '-local')
      results_indexed_mr = `hadoop fs -cat results_#{sindex}_local | sort`.lines.to_a
      raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_mr, results_heap_local)
    end
  end
end


def knn_query(input, output, point, k, extra_args="")
  shape = File.extname(input)[1..-1]
  system_check "shadoop knn #{ExtraConfigParams} #{input} #{output} shape:#{shape} point:#{point} k:#{k} #{extra_args} -overwrite"
end

def test_knn
  shape = 'point'
  k = 1000
  point = "700,232"

  # Try with heap files
  heap_file = generate_file('test', shape)
  knn_query(heap_file, 'knn_heap_local', point, k, '-local')
  results_heap_local = `hadoop fs -cat 'knn_heap_local'`.lines.to_a
  knn_query(heap_file, 'knn_heap_mr', point, k, '-no-local')
  results_heap_mr = `hadoop fs -cat 'knn_heap_mr/part*'`.lines.to_a
  raise "Results of range query with local and MapReduce implementations differ" unless array_equal?(results_heap_local, results_heap_mr)
  
  # Try with indexed files
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file = index_file(heap_file, sindex)
    
    # Run knn on the heap file and make sure it gives the same result as before
    knn_query(indexed_file, "results_#{sindex}_local", point, k, '-local')
    results_indexed_local = `hadoop fs -cat results_#{sindex}_local `.lines.to_a
    raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_local, results_heap_local)
      
    knn_query(indexed_file, "results_#{sindex}_mr", point, k, '-no-local')
    results_indexed_mr = `hadoop fs -cat results_#{sindex}_mr/part* `.lines.to_a
    raise "Results of #{sindex} file does not match the heap file" unless array_equal?(results_indexed_mr, results_heap_local) 
  end
end

def spatial_join(method, file1, file2, output, extra_args = '')
  shape = File.extname(file1)[1..-1]
  system_check "shadoop #{method} #{ExtraConfigParams} #{file1} #{file2} #{output} shape:#{shape} #{extra_args} -overwrite"
end

def test_spatial_join
  # Try with heap files
  heap_file1 = generate_file('test1', 'rect')
  heap_file2 = generate_file('test2', 'rect')

  spatial_join('sjmr', heap_file1, heap_file2, 'sjmr_heap')
  sjmr_heap_results = `hadoop fs -cat sjmr_heap/part* | sort`.lines.to_a
  spatial_join('dj', heap_file1, heap_file2, 'bnlj')
  bnlj_results = `hadoop fs -cat bnlj/part* | sort`.lines.to_a
  raise "Results of SJMR and BNLJ on heap files do not match" unless array_equal?(sjmr_heap_results, bnlj_results)

  # Try with indexes (same index for both files)
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file1 = index_file(heap_file1, sindex)
    indexed_file2 = index_file(heap_file2, sindex)

    # Run both SJMR and DJ on indexed files and check the result
    spatial_join('sjmr', indexed_file1, indexed_file2, "sjmr_#{sindex}")
    sjmr_indexed_results = `hadoop fs -cat sjmr_#{sindex}/part* | sort`.lines.to_a
    raise "SJMR results with #{sindex} file does not match the heap file" unless array_equal?(sjmr_indexed_results, sjmr_heap_results) 
    
    spatial_join('dj', indexed_file1, indexed_file2, "dj_#{sindex}")
    dj_indexed_results = `hadoop fs -cat dj_#{sindex}/part* | sort`.lines.to_a
    raise "Distributed Join results with #{sindex} file does not match the heap file" unless array_equal?(dj_indexed_results, bnlj_results)

    # Try one indexed file and one non-indexed file
    spatial_join('sjmr', indexed_file1, heap_file2, "sjmr_#{sindex}_heap")
    sjmr_one_side_results = `hadoop fs -cat sjmr_#{sindex}_heap/par* | sort`.lines.to_a
    raise "SJMR results with one heap file and one #{sindex} file" unless array_equal?(sjmr_one_side_results, sjmr_heap_results)
    
    spatial_join('dj', indexed_file1, heap_file2, "dj_#{sindex}_heap")
    dj_one_side_results = `hadoop fs -cat dj_#{sindex}_heap/par* | sort`.lines.to_a
    raise "DJ results with one heap file and one #{sindex} file" unless array_equal?(dj_one_side_results, bnlj_results)

    # Try one indexed file with a heap file (direct file not a directory struct)
    heap_file_name = `hadoop fs -ls #{heap_file2}`.lines.grep(/data/).first.split.grep(/data/).first
    spatial_join('dj', indexed_file1, heap_file_name, "dj_#{sindex}_heap")
    dj_one_side_results = `hadoop fs -cat dj_#{sindex}_heap/par* | sort`.lines.to_a
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
  public static void main(String[] args) throws java.io.IOException {
    edu.umn.cs.spatialHadoop.operations.FileMBR.main(args);
  }
}
    JAVA
  end
  
  # Compile the class
  required_jars = []
  required_jars += Dir.glob(File.join("lib", "spatialhadoop*.jar"))
  required_jars += Dir.glob("hadoop-core*jar")
  system_check "javac -cp #{required_jars.join(File::PATH_SEPARATOR)} #{source_filename}"
  class_filename = source_filename.sub('.java', '.class')
  
  # Create the jar file
  jar_file = File.join(temp_dir, "custom_point.jar")
  system_check "jar cf #{jar_file} -C #{temp_dir} #{File.basename(class_filename)}"
  
  # Create a test file with points and try with the custom jar file
  test_file = generate_file('test', 'point')
  system_check "hadoop jar #{jar_file} CustomPoint #{test_file}/data_00001 shape:CustomPoint -no-local"
  system_check "hadoop jar #{jar_file} CustomPoint #{test_file}/data_00001 shape:CustomPoint -local"
end

def plot(input, output, extra_args="")
  shape = File.extname(input)[1..-1]
  system_check "shadoop plot #{ExtraConfigParams} #{input} #{output} shape:#{shape} #{extra_args} -overwrite"
end

def test_plot
  shape = 'rect'
  rect = "100,100,8000,9000"
  
  # Try with heap files
  heap_file = generate_file('test', shape)
  plot(heap_file, 'heap_local.png', '-local')
  plot(heap_file, 'heap_mr.png', '-no-local')
  
  # Try with indexed files
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file = index_file(heap_file, sindex)
    plot(indexed_file, 'indexed_local.png', '-local')
    plot(indexed_file, 'indexed_mr.png', '-no-local')
  end
  
end

# Main
if $0 == __FILE__
  test_range_query
  test_knn
  test_spatial_join
  test_custom_class
  test_plot
end
