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

def generate_file(prefix, shape)
  filename = "#{prefix}.#{shape}"
  system_check "shadoop generate shape:#{shape} '#{filename}' size:200.mb mbr:0,0,1000000,1000000 -overwrite"
  filename
end

def index_file(filename, sindex)
  shape = File.extname(filename)[1..-1]
  indexed_filename = filename.dup; indexed_filename[-File.extname(filename).length, 0] = ".#{sindex}"
  system_check "shadoop index #{filename} #{indexed_filename} shape:#{shape} sindex:#{sindex} -overwrite"
  indexed_filename
end

def range_query(input, output, query, extra_args)
  shape = File.extname(input)[1..-1]
  system_check "shadoop rangequery #{input} #{output} shape:#{shape} rect:#{query} #{extra_args} -overwrite"
end

def run_tests
  # Try range query with heap files
  heap_file = generate_file('test', 'point')
  query = '40,990,1000,8000'
  range_query(heap_file, 'results_mr', query, '-no-local')
  results_rq = `hadoop fs -cat results_mr/part*`.lines.to_a.sort
  range_query(heap_file, 'results_local', query, '-local')
  results_local = `hadoop fs -cat results_local`.lines.to_a.sort
  raise "Results of local and MapReduce implementations are different" if results_rq != results_local
  
  # Try with indexed files
  %w(grid rtree r+tree str str+).each do |sindex|
    indexed_file = index_file(heap_file, sindex)
    
    # Make sure the indexed file has the same number of records as the heap file
    if sindex != 'rtree' && sindex != 'r+tree'
      heap_file_count = `hadoop fs -cat #{heap_file}/part* | wc -l`
      if sindex == 'str+'
        indexed_file_count = `hadoop fs -cat #{indexed_file}/part* | sort | uniq | wc -l`
      else
        indexed_file_count = `hadoop fs -cat #{indexed_file}/part* | wc -l`
      end
      raise "#{sindex} index size #{indexed_file_count} should be equal to heap file size #{heap_file_count}" unless heap_file_count == indexed_file_count
    end
    
    # Run range query on the heap file and make sure it gives the same result as before
    range_query(indexed_file, "results_#{sindex}_mr", query, '-no-local')
    results_indexed_local = `hadoop fs -cat results_#{sindex}_mr/part*`.lines.to_a.sort
    raise "Results of #{sindex} file does not match the heap file" if results_indexed_local.size != results_local.size
      
    range_query(indexed_file, "results_#{sindex}_local", query, '-local')
    results_indexed_mr = `hadoop fs -cat results_#{sindex}_local`.lines.to_a.sort
    raise "Results of #{sindex} file does not match the heap file" if results_indexed_mr.size != results_local.size 
  end
  
end

# Main
if $0 == __FILE__
  run_tests
end