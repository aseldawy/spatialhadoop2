package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.LSMRTreeIndexer;
import edu.umn.cs.spatialHadoop.io.Text2;

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

	private static void generateBenchmarkQueries(Rectangle mbr, double ratio, int count) throws IOException {
		final byte[] NewLine = new byte[] { '\n' };
		Configuration conf = new Configuration();
		Random rand = new Random();

		double offsetX = (mbr.x2 - mbr.x1) * ratio;
		double offsetY = (mbr.y2 - mbr.y1) * ratio;

		FileSystem fs = FileSystem.get(conf);
		Path benchmarkPath = new Path("benchmarks");
		if (!fs.exists(benchmarkPath)) {
			fs.mkdirs(benchmarkPath);
		}
		String fileName = String.format("benchmark_%f_%d.txt", ratio, count);
		Path filePath = new Path(benchmarkPath, fileName);
		OutputStream benchmarkOut = fs.create(filePath);
		for (int i = 0; i < count; i++) {
			double randX = mbr.x1 + (mbr.x2 - mbr.x1 - offsetX) * rand.nextDouble();
			double randY = mbr.y1 + (mbr.y2 - mbr.y1 - offsetY) * rand.nextDouble();
			Rectangle rect = new Rectangle(randX, randY, randX + offsetX, randY + offsetY);
			Text tempLine = new Text2();
			rect.toText(tempLine);
			benchmarkOut.write(tempLine.getBytes(), 0, tempLine.getLength());
			benchmarkOut.write(NewLine);
		}
		benchmarkOut.close();
	}
	
	private static void generateBenchmarkQueries2(Rectangle mbr, double ratio, int count) throws IOException {
		final byte[] NewLine = new byte[] { '\n' };
		Configuration conf = new Configuration();
		Random rand = new Random();

		double offsetX = (mbr.x2 - mbr.x1) * ratio;
		double offsetY = (mbr.y2 - mbr.y1) * ratio;

		FileSystem fs = FileSystem.get(conf);
		Path benchmarkPath = new Path("benchmarks2");
		if (!fs.exists(benchmarkPath)) {
			fs.mkdirs(benchmarkPath);
		}
		
		String fileName = String.format("benchmark_%f_%d.txt", ratio, count);
		Path filePath = new Path(benchmarkPath, fileName);
		OutputStream benchmarkOut = fs.create(filePath);
	    ArrayList<Point> randomPoints = new ArrayList<Point>();
	    Path randomPointsPath = new Path(benchmarkPath, "random_points.txt");
	    LineReader in = new LineReader(fs.open(randomPointsPath));
		Text tempLine = new Text2();
		while(in.readLine(tempLine) > 0) {
			Point p = new Point();
			p.fromText(tempLine);
			Rectangle rect = new Rectangle(p.x, p.y, p.x + offsetX, p.y + offsetY);
			rect.toText(tempLine);
			benchmarkOut.write(tempLine.getBytes(), 0, tempLine.getLength());
			benchmarkOut.write(NewLine);
		}
		
//		for (int i = 0; i < count; i++) {
//			double randX = mbr.x1 + (mbr.x2 - mbr.x1 - offsetX) * rand.nextDouble();
//			double randY = mbr.y1 + (mbr.y2 - mbr.y1 - offsetY) * rand.nextDouble();
//			Rectangle rect = new Rectangle(randX, randY, randX + offsetX, randY + offsetY);
//			Text tempLine = new Text2();
//			rect.toText(tempLine);
//			benchmarkOut.write(tempLine.getBytes(), 0, tempLine.getLength());
//			benchmarkOut.write(NewLine);
//		}
		benchmarkOut.close();
	}

//	private static long lsmRangeQuery(Path inPath, Rectangle queryRange, Shape shape, OperationsParams params,
//			ResultCollector<Shape> output) throws IOException, InterruptedException {
//		HashSet<String> components = LSMRTreeIndexer.getComponentList(inPath);
//		int resultCount = 0;
//		for (String component : components) {
//			Path componentPath = new Path(inPath, component);
//			resultCount += RangeQuery.rangeQueryLocal(inPath, queryRange, shape, params, output);
//		}
//
//		return resultCount;
//	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
	    
//		 generateBenchmarkQueries2(new Rectangle(-180,-90,180,83), 0.1, 1000);
//		 generateBenchmarkQueries2(new Rectangle(-180,-90,180,83), 0.01, 1000);
//		 generateBenchmarkQueries2(new Rectangle(-180,-90,180,83), 0.001,
//		 1000);
//		 generateBenchmarkQueries2(new Rectangle(-180,-90,180,83), 0.0001, 1000);
	    
	    final Path[] paths = params.getPaths();
	    if (paths.length <= 1 && !params.checkInput()) {
	      printUsage();
	      System.exit(1);
	    }
	    if (paths.length >= 2 && !params.checkInputOutput()) {
	      printUsage();
	      System.exit(1);
	    }
//	    if (params.get("rect") == null) {
//	      System.err.println("You must provide a query range");
//	      printUsage();
//	      System.exit(1);
//	    }
	    
	    Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		int count = Integer.parseInt(params.get("count", "5"));
		double ratio = Double.parseDouble(params.get("ratio", "0.0001"));
		
		StringBuilder rectsBuilder = new StringBuilder();
		Path benchmarkPath = new Path(String.format("benchmarks2/benchmark_%f_%d.txt", ratio, 1000));
		LineReader in = new LineReader(fs.open(benchmarkPath));
		Text tempLine = new Text2();
		for (int i = 0; i < count; i++) {
			if (in.readLine(tempLine) > 0) {
				if(i != 0)
					rectsBuilder.append("\n");
				rectsBuilder.append(tempLine.toString());
			}
		}
		System.out.println("rect = " + rectsBuilder.toString());
		params.set("rect", rectsBuilder.toString());
		
	    final Path inPath = params.getInputPath();
	    final Path outPath = params.getOutputPath();
	    final Rectangle[] queryRanges = params.getShapes("rect", new Rectangle());
	    System.out.println("Query range size = " + queryRanges.length);
	    for(Rectangle r: queryRanges) {
	    	System.out.println(r.toString());
	    }

	    // All running jobs
	    final Vector<Long> resultsCounts = new Vector<Long>();
	    Vector<Job> jobs = new Vector<Job>();
	    Vector<Thread> threads = new Vector<Thread>();

	    long t1 = System.currentTimeMillis();
	    for (int i = 0; i < queryRanges.length; i++) {
	      final OperationsParams queryParams = new OperationsParams(params);
	      OperationsParams.setShape(queryParams, "rect", queryRanges[i]);
	      if (OperationsParams.isLocal(new JobConf(queryParams), inPath)) {
	        // Run in local mode
	        final Rectangle queryRange = queryRanges[i];
	        final Shape shape = queryParams.getShape("shape");
	        final Path output = outPath == null ? null :
	          (queryRanges.length == 1 ? outPath : new Path(outPath, String.format("%05d", i)));
	        Thread thread = new Thread() {
	          @Override
	          public void run() {
	            FSDataOutputStream outFile = null;
	            final byte[] newLine = System.getProperty("line.separator", "\n").getBytes();
	            try {
	              ResultCollector<Shape> collector = null;
	              if (output != null) {
	                FileSystem outFS = output.getFileSystem(queryParams);
	                final FSDataOutputStream foutFile = outFile = outFS.create(output);
	                collector = new ResultCollector<Shape>() {
	                  final Text tempText = new Text2();
	                  @Override
	                  public synchronized void collect(Shape r) {
	                    try {
	                      tempText.clear();
	                      r.toText(tempText);
	                      foutFile.write(tempText.getBytes(), 0, tempText.getLength());
	                      foutFile.write(newLine);
	                    } catch (IOException e) {
	                      e.printStackTrace();
	                    }
	                  }
	                };
	              } else {
	                outFile = null;
	              }
	              long resultCount = RangeQuery.rangeQueryLocal(inPath, queryRange, shape, queryParams, collector);
	              resultsCounts.add(resultCount);
	            } catch (IOException e) {
	              e.printStackTrace();
	            } catch (InterruptedException e) {
	              e.printStackTrace();
	            } finally {
	              try {
	                if (outFile != null)
	                  outFile.close();
	              } catch (IOException e) {
	                e.printStackTrace();
	              }
	            }
	          }
	        };
	        thread.start();
	        threads.add(thread);
	      } else {
	        // Run in MapReduce mode
	        queryParams.setBoolean("background", true);
	        Job job = RangeQuery.rangeQueryMapReduce(inPath, outPath, queryParams);
	        jobs.add(job);
	      }
	    }

	    while (!jobs.isEmpty()) {
	      Job firstJob = jobs.firstElement();
	      firstJob.waitForCompletion(false);
	      if (!firstJob.isSuccessful()) {
	        System.err.println("Error running job "+firstJob);
	        System.err.println("Killing all remaining jobs");
	        for (int j = 1; j < jobs.size(); j++)
	          jobs.get(j).killJob();
	        System.exit(1);
	      }
	      Counters counters = firstJob.getCounters();
	      Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
	      resultsCounts.add(outputRecordCounter.getValue());
	      jobs.remove(0);
	    }
	    while (!threads.isEmpty()) {
	      try {
	        Thread thread = threads.firstElement();
	        thread.join();
	        threads.remove(0);
	      } catch (InterruptedException e) {
	        e.printStackTrace();
	      }
	    }
	    long t2 = System.currentTimeMillis();
	    
	    System.out.println("Time for "+queryRanges.length+" jobs in  ms is " + (t2-t1));
	    System.out.println("Results counts: "+resultsCounts);
	  }

//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//
//		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.1, 1000);
//		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.01, 1000);
//		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.001,
//		 1000);
//		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.0001,
//		 1000);
//
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//
//		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
//
//		final Path[] paths = params.getPaths();
//		if (paths.length <= 1 && !params.checkInput()) {
//			printUsage();
//			System.exit(1);
//		}
//		if (paths.length >= 2 && !params.checkInputOutput()) {
//			printUsage();
//			System.exit(1);
//		}
//		// if (params.get("rect") == null) {
//		// System.err.println("You must provide a query range");
//		// printUsage();
//		// System.exit(1);
//		// }
//
//		String query = params.get("query", "rangequery");
//		int count = Integer.parseInt(params.get("count", "5"));
//		double ratio = Double.parseDouble(params.get("ratio", "0.0001"));
//
//		final Path inPath = params.getInputPath();
//		final Path outPath = params.getOutputPath();
//
//		Rectangle[] queryRanges = new Rectangle[count];
//		StringBuilder rectsBuilder = new StringBuilder();
//		Path benchmarkPath = new Path(String.format("benchmarks/benchmark_%f_%d.txt", ratio, 1000));
//		LineReader in = new LineReader(fs.open(benchmarkPath));
//		Text tempLine = new Text2();
//		for (int i = 0; i < count; i++) {
//			if (in.readLine(tempLine) > 0) {
//				Rectangle rect = new Rectangle();
//				rect.fromText(tempLine);
//				queryRanges[i] = rect;
//				// if(i != 0) {
//				// rectsBuilder.append("\n");
//				// }
//				// rectsBuilder.append(in.toString());
//			}
//		}
//		// System.out.println("rect string is " + rectsBuilder.toString());
//		// params.set("rect", rectsBuilder.toString());
//		// final Rectangle[] queryRanges = params.getShapes("rect", new
//		// Rectangle());
//		System.out.println("Query range size = " + queryRanges.length);
//		for (Rectangle r : queryRanges) {
//			System.out.println(r.toString());
//		}
//
//		// All running jobs
//		final Vector<Long> resultsCounts = new Vector<Long>();
//		Vector<Job> jobs = new Vector<Job>();
//		Vector<Thread> threads = new Vector<Thread>();
//
//		long t1 = System.currentTimeMillis();
//		for (int i = 0; i < queryRanges.length; i++) {
//			final OperationsParams queryParams = new OperationsParams(params);
//			queryParams.setBoolean("output", false);
//			OperationsParams.setShape(queryParams, "rect", queryRanges[i]);
//			// Run in MapReduce mode
//			queryParams.setBoolean("background", true);
//			Job job = RangeQuery.rangeQueryMapReduce(inPath, outPath, queryParams);
//			jobs.add(job);
//		}
//
//		while (!jobs.isEmpty()) {
//			Job firstJob = jobs.firstElement();
//			firstJob.waitForCompletion(false);
//			if (!firstJob.isSuccessful()) {
//				System.err.println("Error running job " + firstJob);
//				System.err.println("Killing all remaining jobs");
//				for (int j = 1; j < jobs.size(); j++)
//					jobs.get(j).killJob();
//				System.exit(1);
//			}
//			Counters counters = firstJob.getCounters();
//			Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
//			resultsCounts.add(outputRecordCounter.getValue());
//			jobs.remove(0);
//		}
//		while (!threads.isEmpty()) {
//			try {
//				Thread thread = threads.firstElement();
//				thread.join();
//				threads.remove(0);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		long t2 = System.currentTimeMillis();
//
//		System.out.println("Time for " + queryRanges.length + " jobs is " + (t2 - t1) + " millis");
//		System.out.println("Results counts: " + resultsCounts);
//		// final OperationsParams params = new OperationsParams(new
//		// GenericOptionsParser(args));
//		// Path[] inputFiles = params.getPaths();
//		//
//		// Configuration conf = new Configuration();
//		// FileSystem fs = FileSystem.get(conf);
//		//
//		// // if (!params.checkInput() || (inputFiles.length != 2)) {
//		// // printUsage();
//		// // System.exit(1);
//		// // }
//		//
//		// generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.1, 1000);
//		// generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.01, 1000);
//		// generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.001,
//		// 1000);
//		// generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.0001,
//		// 1000);
//		//
//		// Path inPath = inputFiles[0];
//		// Path outPath = inputFiles[1];
//		// System.out.println("Input path: " + inPath);
//		// System.out.println("Output path: " + outPath);
//		// String query = params.get("query", "rangequery");
//		// int count = Integer.parseInt(params.get("count", "5"));
//		// final Shape shape = params.getShape("shape");
//		// double ratio = Double.parseDouble(params.get("ratio", "0.0001"));
//		// if (query.equals("rangequery")) {
//		// params.setBoolean("output", false);
//		// StringBuilder rectsBuilder = new StringBuilder();
//		// Path benchmarkPath = new
//		// Path(String.format("benchmarks/benchmark_%f_%d.txt", ratio, 1000));
//		// LineReader in = new LineReader(fs.open(benchmarkPath));
//		// Text tempLine = new Text2();
//		// for (int i = 0; i < count; i++) {
//		// if (in.readLine(tempLine) > 0) {
//		// Rectangle rect = new Rectangle();
//		// rect.fromText(tempLine);
//		// if(i != 0) {
//		// rectsBuilder.append("\n");
//		// }
//		// rectsBuilder.append(rect.toString());
//		//// ResultCollector<Shape> collector = null;
//		// // long resultCount = RangeQuery.rangeQueryLocal(inPath,
//		// // rect, shape, params, collector);
//		//// params.set("rect", rect.toString());
//		//
//		//// long resultCount = lsmRangeQuery(inPath, rect, shape, params,
//		// collector);
//		//// System.out.println("Result count = " + resultCount);
//		// }
//		// }
//		//
//		// params.set("rect", rectsBuilder.toString());
//		//
//		// long t1 = System.currentTimeMillis();
//		// RangeQuery.rangeQueryMapReduce(inPath, outPath, params);
//		// long t2 = System.currentTimeMillis();
//		// System.out.println("Number of queries = " + count + ", total time: "
//		// + (t2 - t1));
//		// } else {
//		// printUsage();
//		// System.exit(1);
//		// }
//	}
}
