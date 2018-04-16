input_path = "/user/tvu032/sorted_osm_32mb/sorted_all_nodes_4G"
index_path = "dynindex"

batches = `hdfs dfs -ls #{input_path}`.each_line.map { |line| line.split[7]}
batches.delete(nil)
batches = batches.sort_by {|f| f.split('_')[-1].to_i }

`hdfs dfs -rm -r #{index_path}`

for batch in batches
  batch_num = batch.split('_')[-1].to_i
  output = `hadoop jar spatialhadoop-2.4.3-SNAPSHOT-uber.jar insert shape:osmpoint gindex:rstar #{batch} #{index_path}`
  puts "batch: #{batch}"
  puts output
  `hdfs dfs -get dynindex/_rstar.wkt dynindex_#{batch_num}.wkt`
  `hdfs dfs -get dynindex/_master.rstar dynindex_#{batch_num}.rstar`
end