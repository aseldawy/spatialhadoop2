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

def range_query(input, output, query, extra_args)
  shape = File.extname(input)[1..-1]
  system_check "shadoop rangequery #{input} #{output} shape:#{shape} rect:#{query} #{extra_args}"
end

def run_tests
  test_file = generate_file('test', 'point')
  range_query(test_file, 'results_rq', '40,990,1000,800', '-no-local')
  results_rq = `hadoop fs -cat results_rq/part*`
  range_query(test_file, 'results_local', '40,990,1000,800', '-local')
  results_local = `hadoop fs -cat results_local`
end

# Main
if $0 == __FILE__
  run_tests
end