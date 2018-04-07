package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.MetadataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
	private static List<List<Partition>> getSplitGroupsWithMaximumReducedCost(
			Path indexPath,
			List<Partition> partitions, OperationsParams params) throws IOException {
		List<List<Partition>> splitGroups;
		
		FileSystem fs = indexPath.getFileSystem(params);
		((FilterFileSystem)fs).getRawFileSystem().setConf(params);
		long blockSize = fs.getDefaultBlockSize(indexPath);
		double overflowRate = params.getDouble("overflow_rate", 1.1);
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
		
		Rectangle mbr = (Rectangle) OperationsParams.getShape(params, "mbr");
		if (mbr == null) {
			mbr = new Rectangle(partitions.get(0));
			for (Partition p : partitions) {
				mbr.expand(p);
			}
		}
		double querySize = 0.000001 * Math.sqrt(mbr.area());
		
		List<Partition> remainingPartitions;
    List<Partition> splitPartitions = new ArrayList<Partition>();
		remainingPartitions = new ArrayList(partitions);

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
	
	private static double computeReducedCost(List<Partition> splittingPartitions, double querySize,
                                           long blockSize) {
		// System.out.println("Computing reduced cost of a set of partitions.");
		// Group splitting partitions by overlapping clusters
		List<List<Partition>> groups = MetadataUtil.groupByOverlappingPartitions(splittingPartitions);

		// System.out.println("Number of groups = " + groups.size());

		// Compute reduced cost
		double costBefore = 0;
		for (Partition p : splittingPartitions) {
			costBefore += (p.getWidth() + querySize) * (p.getHeight() + querySize) * p.getNumberOfBlock(blockSize);
		}

		double costAfter = 0;
		for (List<Partition> group : groups) {
			double groupBlocks = 0;
			Partition tempPartition = group.get(0).clone();
			for (Partition p : group) {
				groupBlocks += p.getNumberOfBlock(blockSize);
				tempPartition.expand(p);
			}
			double groupArea = tempPartition.area();

			costAfter += Math.pow((Math.sqrt(groupArea / groupBlocks) + querySize), 2) * groupBlocks;
		}

		// System.out.println("Reduced cost = " + Math.abs(costBefore - costAfter));
		return Math.abs(costBefore - costAfter);
	}
	
	private static Partition findBestCandidateToReduceCost(List<Partition> currentPartitions,
                                                         List<Partition> splittingPartitions, double querySize, long blockSize) {
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
	private static List<List<Partition>> getSplitGroupsWithMaximumReducedArea(List<Partition> partitions, OperationsParams params) throws IOException {
		List<List<Partition>> splitGroups;
		
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
		
		List<Partition> splitPartitions = new ArrayList<Partition>();
    List<Partition> remainingPartitions = new ArrayList<Partition>(partitions);
		
		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceArea(remainingPartitions, splitPartitions);
			splitPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}
		
		splitGroups = MetadataUtil.groupByOverlappingPartitions(splitPartitions);
		return splitGroups;
	}
	
	private static double computeReducedArea(List<Partition> splittingPartitions, Partition partition) {
		double reducedArea = partition.area();
		for(Partition p: splittingPartitions) {
			if(p.isIntersected(partition)) {
				reducedArea += p.getIntersection(partition).area();
			}
		}
		return reducedArea;
 	}
	
	private static Partition findBestCandidateToReduceArea(List<Partition> currentPartitions,
                                                         List<Partition> splittingPartitions) {
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
	
	public static List<List<Partition>> getSplitGroups(Path indexPath, OperationsParams params, OptimizerType type) throws IOException {
		List<List<Partition>> splitGroups;
		
		List<Partition> partitions = MetadataUtil.getPartitions(indexPath, params);
		
		if(type == OptimizerType.MaximumReducedCost) {
			splitGroups = getSplitGroupsWithMaximumReducedCost(indexPath, partitions, params);
		} else if(type == OptimizerType.MaximumReducedArea) {
			splitGroups = getSplitGroupsWithMaximumReducedArea(partitions, params);
		} else {
			throw new RuntimeException("Unknown optimizer type " + type);
		}
		
		return splitGroups;
	}
}
