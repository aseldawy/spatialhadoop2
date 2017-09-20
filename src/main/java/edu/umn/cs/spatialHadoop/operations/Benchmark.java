package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
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

	private static long lsmRangeQuery(Path inPath, Rectangle queryRange, Shape shape, OperationsParams params,
			ResultCollector<Shape> output) throws IOException, InterruptedException {
		HashSet<String> components = LSMRTreeIndexer.getComponentList(inPath);
		int resultCount = 0;
		for(String component: components) {
			Path componentPath = new Path(inPath, component);
			resultCount += RangeQuery.rangeQueryLocal(inPath, queryRange, shape, params, output);
		}
		
		return resultCount;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// if (!params.checkInput() || (inputFiles.length != 2)) {
		// printUsage();
		// System.exit(1);
		// }

		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.1, 1000);
		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.01, 1000);
		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.001,
		 1000);
		 generateBenchmarkQueries(new Rectangle(-180,-90,180,83), 0.0001, 1000);

//		Path inPath = inputFiles[0];
//		Path outPath = inputFiles[1];
//		System.out.println("Input path: " + inPath);
//		System.out.println("Output path: " + outPath);
//		String query = params.get("query", "rangequery");
//		int count = Integer.parseInt(params.get("count", "5"));
//		final Shape shape = params.getShape("shape");
//		double ratio = Double.parseDouble(params.get("ratio", "0.0001"));
//		if (query.equals("rangequery")) {
//			long t1 = System.currentTimeMillis();
//			Path benchmarkPath = new Path(String.format("benchmarks/benchmark_%f_%d.txt", ratio, 1000));
//			LineReader in = new LineReader(fs.open(benchmarkPath));
//			Text tempLine = new Text2();
//			for (int i = 0; i < count; i++) {
//				if (in.readLine(tempLine) > 0) {
//					Rectangle rect = new Rectangle();
//					rect.fromText(tempLine);
//					ResultCollector<Shape> collector = null;
//					// long resultCount = RangeQuery.rangeQueryLocal(inPath, rect, shape, params, collector);
//					long resultCount = lsmRangeQuery(inPath, rect, shape, params, collector);
//					System.out.println("Result count = " + resultCount);
//				}
//			}
//
//			long t2 = System.currentTimeMillis();
//			System.out.println("Number of queries = " + count + ", total time: " + (t2 - t1) + "ms");
//		} else {
//			printUsage();
//			System.exit(1);
//		}
	}
}
