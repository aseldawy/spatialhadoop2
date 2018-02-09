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
	
	private static double computeReducedCost(ArrayList<Partition> splittingPartitions, double querySize,
			int blockSize) {
		// System.out.println("Computing reduced cost of a set of partitions.");
		// Group splitting partitions by overlapping clusters
		ArrayList<ArrayList<Partition>> groups = new ArrayList<ArrayList<Partition>>();
		@SuppressWarnings("unchecked")
		ArrayList<Partition> tempSplittingPartitions = (ArrayList<Partition>) splittingPartitions.clone();

		while (tempSplittingPartitions.size() > 0) {
			ArrayList<Partition> group = new ArrayList<Partition>();
			group.add(tempSplittingPartitions.get(0));
			for (Partition p : tempSplittingPartitions) {
				if (MetadataUtil.isOverlapping(group, p)) {
					group.add(p);
				}
			}
			groups.add(group);
			tempSplittingPartitions.removeAll(group);
		}

		// System.out.println("Number of groups = " + groups.size());

		// Compute reduced cost
		double costBefore = 0;
		for (Partition p : splittingPartitions) {
			costBefore += (p.getWidth() + querySize) * (p.getHeight() + querySize) * p.getNumberOfBlock(blockSize);
		}

		double costAfter = 0;
		for (ArrayList<Partition> group : groups) {
			double groupBlocks = 0;
			Partition tempPartition = group.get(0).clone();
			for (Partition p : group) {
				groupBlocks += p.getNumberOfBlock(blockSize);
				tempPartition.expand(p);
			}
			double groupArea = tempPartition.getSize();

			costAfter += Math.pow((Math.sqrt(groupArea / groupBlocks) + querySize), 2) * groupBlocks;
		}

		// System.out.println("Reduced cost = " + Math.abs(costBefore - costAfter));
		return Math.abs(costBefore - costAfter);
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
