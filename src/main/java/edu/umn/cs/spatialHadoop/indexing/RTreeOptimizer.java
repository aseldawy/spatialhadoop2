package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;

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
	@SuppressWarnings("unchecked")
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedCost(ArrayList<Partition> partitions, OperationsParams params) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");
//		double budget = Double.parseDouble(params.get("budget")) * 1024 * 1024;
//		int budgetBlocks = (int) Math.ceil(budget / blockSize);
		
		long incrementalRTreeBudget = 0;
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				incrementalRTreeBudget += p.size;
			}
		}
		int budgetBlocks = (int) Math.ceil((float)incrementalRTreeBudget / (float)blockSize);
		
		Rectangle mbr = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (mbr == null) {
			mbr = new Rectangle(partitions.get(0));
			for (Partition p : partitions) {
				mbr.expand(p);
			}
		}
		double querySize = 0.000001 * Math.sqrt(mbr.getSize());
		
		ArrayList<Partition> remainingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		remainingPartitions = (ArrayList<Partition>) partitions.clone();

		// Find the partition with maximum reduced cost as the seed for our greedy algorithm
//		Partition maxReducedCostPartition = partitions.get(0);
//		double maxReducedCost = 0;
//		for (Partition p : partitions) {
//			splitPartitions.add(p);
//			double pReducedCost = computeReducedCost(splitPartitions, querySize, blockSize);
//			if (maxReducedCost < pReducedCost) {
//				maxReducedCost = pReducedCost;
//				maxReducedCostPartition = p;
//			}
//			splitPartitions.remove(p);
//		}
		
//		Partition maxReducedCostPartition = findBestCandidateToReduceCost(remainingPartitions, splitPartitions, querySize, blockSize);
//		splitPartitions.add(maxReducedCostPartition);
//		remainingPartitions.remove(maxReducedCostPartition);
//		budgetBlocks -= maxReducedCostPartition.getNumberOfBlock(blockSize);

		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceCost(remainingPartitions, splitPartitions,
					querySize, blockSize);
			splitPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}
		
		splitGroups = MetadataUtil.groupByOverlappingPartitions(splitPartitions);
		return splitGroups;
	}
	
	private static double computeReducedCost(ArrayList<Partition> splittingPartitions, double querySize,
			int blockSize) {
		// System.out.println("Computing reduced cost of a set of partitions.");
		// Group splitting partitions by overlapping clusters
		ArrayList<ArrayList<Partition>> groups = MetadataUtil.groupByOverlappingPartitions(splittingPartitions);

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
	
	private static Partition findBestCandidateToReduceCost(ArrayList<Partition> currentPartitions,
			ArrayList<Partition> splittingPartitions, double querySize, int blockSize) {
		Partition bestPartition = currentPartitions.get(0);
		double maxReducedCost = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double splittingReducedCost = computeReducedCost(splittingPartitions, querySize, blockSize);
			if (maxReducedCost < splittingReducedCost) {
				bestPartition = p;
				maxReducedCost = splittingReducedCost;
			}
			splittingPartitions.remove(p);
		}

		return bestPartition;
	}
	
	// Greedy algorithm that maximize the reduced area
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedArea(ArrayList<Partition> partitions, OperationsParams params) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");
//		double budget = Double.parseDouble(params.get("budget")) * 1024 * 1024;
//		int budgetBlocks = (int) Math.ceil(budget / blockSize);
		
		long incrementalRTreeBudget = 0;
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				incrementalRTreeBudget += p.size;
			}
		}
		int budgetBlocks = (int) Math.ceil((float)incrementalRTreeBudget / (float)blockSize);
		
		ArrayList<Partition> remainingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		remainingPartitions = (ArrayList<Partition>) partitions.clone();
		
		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceArea(remainingPartitions, splitPartitions);
			splitPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}
		
		splitGroups = MetadataUtil.groupByOverlappingPartitions(splitPartitions);
		return splitGroups;
	}
	
	private static double computeReducedArea(ArrayList<Partition> splittingPartitions, Partition partition) {
		double reducedArea = partition.getSize();
		for(Partition p: splittingPartitions) {
			if(p.isIntersected(partition)) {
				reducedArea += p.getIntersection(partition).getSize();
			}
		}
		return reducedArea;
 	}
	
	private static Partition findBestCandidateToReduceArea(ArrayList<Partition> currentPartitions,
			ArrayList<Partition> splittingPartitions) {
		Partition bestPartition = currentPartitions.get(0);
		double maxArea = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double splittingReducedArea = computeReducedArea(splittingPartitions, p);
			if (maxArea < splittingReducedArea) {
				bestPartition = p;
				maxArea = splittingReducedArea;
			}
			splittingPartitions.remove(p);
		}
		
		return bestPartition;
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
