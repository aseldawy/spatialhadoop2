# Measure the quality of the index given by the input partitions

partitions = $stdin.each_line.map do |line|
  parts = line.split ','
  {
   :id => parts[0].to_i,
   :mbr => parts[1..4].map(&:to_f),
   :num_records => parts[-3].to_i,
   :num_bytes => parts[-2].to_i,
   :filename => parts[-1]
  }
end

query_width = 0.1
query_height = 0.1

# Measure the quality of the index as the expected number of bytes to process
# for a query of the given size query_width x query_height
expected_bytes = 0.0

all_mbrs = partitions.map{|p| p[:mbr]}
overall_mbr = [ all_mbrs.map{|mbr| mbr[0]}.min,
  all_mbrs.map{|mbr| mbr[1]}.min,
  all_mbrs.map{|mbr| mbr[2]}.max,
  all_mbrs.map{|mbr| mbr[3]}.max]

overall_mbr_area = (overall_mbr[2] - overall_mbr[0]) - (overall_mbr[3] - overall_mbr[1])

for partition in partitions
  # Compute the probability of this partition being selected
  partition_mbr = partition[:mbr]
  mbr_width = partition_mbr[2] - partition_mbr[0]
  mbr_height = partition_mbr[3] - partition_mbr[1]
  area_overlap = (mbr_width + query_width) * (mbr_height + query_height)
  probability_selection = area_overlap.to_f / overall_mbr_area
  expected_bytes = probability_selection * partition[:num_bytes]
end

puts "Overall quality = #{expected_bytes}"