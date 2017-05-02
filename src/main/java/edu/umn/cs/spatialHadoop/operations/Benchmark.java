package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

public class Benchmark {
	private static void printUsage() {
		System.out.println("Benchmark query performance");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<in file> - (*) Path to input file");
		System.out.println("<out file> - (*) Path to output file");
		System.out.println("<query> - (*) Name of the benchmarking query");
		System.out.println("<count> - (*) Number of query");
		System.out.println("<ratio> - (*) Selection ratio"); 
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
		final Shape shape = params.getShape("shape");
		double ratio = Double.parseDouble(params.get("ratio", "0.0001"));
		long resultCount = 0;
		if(query.equals("rangequery")) {
			Random rand = new Random();
			long t1 = System.currentTimeMillis();
			for(int i = 0; i < count; i++) {
				int randomX = -90 - rand.nextInt(30);
				int randomY = 30 + rand.nextInt(10);
				String rect = String.format("%.5f,%.5f,%.5f,%.5f", (double)randomX, (double)randomY, (double)(randomX + ratio * 170), (double)(randomY + ratio * 70));
				System.out.println("rect = " + rect);
				params.set("rect", rect);
				Path queryOutPath = new Path(outPath, Integer.toString(i));
				ResultCollector<Shape> collector = null;
				Rectangle queryRange = new Rectangle((double)randomX, (double)randomY, (double)(randomX + ratio * 170), (double)(randomY + ratio * 70));
				resultCount = RangeQuery.rangeQueryLocal(inPath, queryRange, shape, params, collector);
			}
			long t2 = System.currentTimeMillis();
			System.out.println("Count = " + count + ", result count = " + resultCount + ", total time: " + (t2 - t1) + "ms");
		} else {
			printUsage();
			System.exit(1);
		}
	}
}
