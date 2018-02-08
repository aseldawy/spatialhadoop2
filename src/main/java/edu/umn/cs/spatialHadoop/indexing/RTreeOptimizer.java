package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import edu.umn.cs.spatialHadoop.OperationsParams;

public class RTreeOptimizer {
	
	enum OptimizerType {
		IncrementalRTree,
		MaximumReducedCost,
		MaximumReducedArea
	}
	
	// Incremental RTree optimizer
	private static ArrayList<Partition> getOverflowPartitions(ArrayList<Partition> partitions, OperationsParams params) throws IOException {
		ArrayList<Partition> overflowPartitions = new ArrayList<Partition>();
		
		Configuration conf = new Configuration();
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				overflowPartitions.add(p);
			}
		}
		
		return overflowPartitions;
	}
	
	// Greedy algorithm that maximize the reduced range query cost
	private static ArrayList<Partition> getSplitPartitionsWithMaximumReducedCost(ArrayList<Partition> partitions, OperationsParams params) {
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		return splitPartitions;
	}
	
	// Greedy algorithm that maximize the reduced area
	private static ArrayList<Partition> getSplitPartitionsWithMaximumReducedArea(ArrayList<Partition> partitions, OperationsParams params) {
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		return splitPartitions;
	}
	
	public static ArrayList<Partition> getSplitPartitions(Path path, OperationsParams params, OptimizerType type) throws IOException {
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(path, params);
		
		if(type == OptimizerType.IncrementalRTree) {
			splitPartitions = getOverflowPartitions(partitions, params);
		} else if(type == OptimizerType.MaximumReducedCost) {
			splitPartitions = getSplitPartitionsWithMaximumReducedCost(partitions, params);
		} else if(type == OptimizerType.MaximumReducedArea) {
			splitPartitions = getSplitPartitionsWithMaximumReducedArea(partitions, params);
		}
		
		return splitPartitions;
	}
}
