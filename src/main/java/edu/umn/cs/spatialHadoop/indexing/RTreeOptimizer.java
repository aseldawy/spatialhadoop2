package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import edu.umn.cs.spatialHadoop.OperationsParams;

public class RTreeOptimizer {
	
	public enum OptimizerType {
		MaximumReducedCost,
		MaximumReducedArea
	}
	
	// Incremental RTree optimizer
	public static ArrayList<Partition> getOverflowPartitions(Path path, OperationsParams params) throws IOException {
		ArrayList<Partition> overflowPartitions = new ArrayList<Partition>();
		
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(path, params);
		
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
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedCost(ArrayList<Partition> partitions, OperationsParams params) {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		return splitGroups;
	}
	
	// Greedy algorithm that maximize the reduced area
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedArea(ArrayList<Partition> partitions, OperationsParams params) {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		return splitGroups;
	}
	
	public static ArrayList<ArrayList<Partition>> getSplitGroups(Path path, OperationsParams params, OptimizerType type) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(path, params);
		
		if(type == OptimizerType.MaximumReducedCost) {
			splitGroups = getSplitGroupsWithMaximumReducedCost(partitions, params);
		} else if(type == OptimizerType.MaximumReducedArea) {
			splitGroups = getSplitGroupsWithMaximumReducedArea(partitions, params);
		}
		
		return splitGroups;
	}
}
