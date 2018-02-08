package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

public class DynamicIndexer {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		
		String splitType = params.get("splittype");
		String currentPathString = params.get("current");
		String appendPathString = params.get("append");
		Path currentPath = new Path(currentPathString);
		Path appendPath = new Path(appendPathString);

		System.out.println("Current index path: " + currentPath);
		System.out.println("Append data path: " + appendPath);
		
		// Append
		long t1 = System.currentTimeMillis();
		RTreeInserter.append(currentPath, appendPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total appending time in millis " + (t2 - t1));
		
		// Optimize then reorganize
		if(splitType.equals("incrtree")) {
			ArrayList<Partition> splitPartitions = RTreeOptimizer.getOverflowPartitions(currentPath, params);
			long t3 = System.currentTimeMillis();
			System.out.println("Total optimizing time in millis " + (t3 - t2));
			
			RTreeReorganizer.reorganizePartition(currentPath, splitPartitions, params);
			
			long t4 = System.currentTimeMillis();
			System.out.println("Total reorganizing time in millis " + (t4 - t3));
		} else {
			ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
			if(splitType.equals("greedyreducedcost")) {
				splitGroups = RTreeOptimizer.getSplitPartitions(currentPath, params, RTreeOptimizer.OptimizerType.MaximumReducedCost);
			} else if(splitType.equals("greedyreducedarea")) {
				splitGroups = RTreeOptimizer.getSplitPartitions(currentPath, params, RTreeOptimizer.OptimizerType.MaximumReducedArea);
			}
			
			long t3 = System.currentTimeMillis();
			System.out.println("Total optimizing time in millis " + (t3 - t2));
			
			RTreeReorganizer.reorganizeGroup(currentPath, splitGroups, params);
			
			long t4 = System.currentTimeMillis();
			System.out.println("Total reorganizing time in millis " + (t4 - t3));
		}
	}
}
