package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

public class Benchmark {
	private static void printUsage() {
		System.out.println("Benchmark query performance");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<in file> - (*) Path to input file");
		System.out.println("<out file> - (*) Path to output file");
		System.out.println("<query> - (*) Name of the benchmarking query");
		System.out.println("<count> - (*) Number of query"); 
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

//		if (!params.checkInput() || (inputFiles.length != 2)) {
//			printUsage();
//			System.exit(1);
//		}

		Path inPath = inputFiles[0];
		Path outPath = inputFiles[1];
		System.out.println("Input path: " + inPath);
		System.out.println("Output path: " + outPath);
		String query = params.get("query", "rangequery");
		int count = Integer.parseInt(params.get("count", "5"));
		if(query.equals("rangequery")) {
			params.set("rect", "1000,1000,2000,2000");
			long t1 = System.currentTimeMillis();
			for(int i = 0; i < count; i++) {
				Path queryOutPath = new Path(outPath, Integer.toString(i));
				RangeQuery.rangeQueryMapReduce(inPath, queryOutPath, params);
			}
			long t2 = System.currentTimeMillis();
			System.out.println("Total time: " + (t2 - t1) + "ms");
		} else {
			printUsage();
			System.exit(1);
		}
	}
}
