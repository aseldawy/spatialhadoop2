package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.RTreeIndexer.InserterMap;
import edu.umn.cs.spatialHadoop.indexing.RTreeIndexer.InserterReduce;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.util.GraphUtils;
import edu.umn.cs.spatialHadoop.util.Node;

public class IncrementalRTreeIndexer {

	private static final Log LOG = LogFactory.getLog(Indexer.class);

	/**
	 * The map function that partitions the data using the configured
	 * partitioner. Refer from Indexer class
	 * 
	 * @author Tin Vu
	 *
	 */
	public static class InserterMap extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private Partitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to
		 * assign it to only one partition
		 */
		private boolean replicate;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.partitioner = Partitioner.getPartitioner(context.getConfiguration());
			this.replicate = context.getConfiguration().getBoolean("replicate", false);
		}

		@Override
		protected void map(Rectangle key, Iterable<? extends Shape> shapes, final Context context)
				throws IOException, InterruptedException {
			final IntWritable partitionID = new IntWritable();
			for (final Shape shape : shapes) {
				if (replicate) {
					partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
						@Override
						public void collect(Integer r) {
							partitionID.set(r);
							try {
								context.write(partitionID, shape);
							} catch (IOException e) {
								LOG.warn("Error checking overlapping partitions", e);
							} catch (InterruptedException e) {
								LOG.warn("Error checking overlapping partitions", e);
							}
						}
					});
				} else {
					partitionID.set(partitioner.overlapPartition(shape));
					if (partitionID.get() >= 0)
						context.write(partitionID, shape);
				}
				context.progress();
			}
		}
	}

	public static class InserterReduce<S extends Shape> extends Reducer<IntWritable, Shape, IntWritable, Shape> {

		@Override
		protected void reduce(IntWritable partitionID, Iterable<Shape> shapes, Context context)
				throws IOException, InterruptedException {
			LOG.info("Working on partition #" + partitionID);
			for (Shape shape : shapes) {
				try {
					context.write(partitionID, shape);
					context.progress();
				} catch (Exception e) {

				}
			}
			// Indicate end of partition to close the file
			context.write(new IntWritable(-partitionID.get() - 1), null);
			LOG.info("Done with partition #" + partitionID);
		}
	}

	private static Job insertMapReduce(Path currentPath, Path appendPath, OperationsParams params) throws IOException,
			InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		@SuppressWarnings("deprecation")
		Job job = new Job(params, "Inserter");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Inserter.class);

		// Set input file MBR if not already set
		Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (inputMBR == null) {
			inputMBR = FileMBR.fileMBR(currentPath, new OperationsParams(conf));
			OperationsParams.setShape(conf, "mbr", inputMBR);
		}

		String index = conf.get("sindex");
		if (index == null)
			throw new RuntimeException("Index type is not set");
		// setLocalIndexer(conf, index);
		// Load the partitioner from file
		RTreeFilePartitioner partitioner = new RTreeFilePartitioner();
		partitioner.createFromMasterFile(currentPath, params);
		Partitioner.setPartitioner(conf, partitioner);

		// Set mapper and reducer
		Shape shape = OperationsParams.getShape(conf, "shape");
		job.setMapperClass(InserterMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setReducerClass(InserterReduce.class);
		// Set input and output
		job.setInputFormatClass(SpatialInputFormat3.class);
		SpatialInputFormat3.setInputPaths(job, appendPath);
		job.setOutputFormatClass(IndexOutputFormat.class);
		Path tempPath = new Path(currentPath, "temp");
		IndexOutputFormat.setOutputPath(job, tempPath);
		// Set number of reduce tasks according to cluster status
		ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
		job.setNumReduceTasks(
				Math.max(1, Math.min(partitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));

		// Use multithreading in case the job is running locally
		conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

		// Start the job
		if (conf.getBoolean("background", false)) {
			// Run in background
			job.submit();
		} else {
			job.waitForCompletion(conf.getBoolean("verbose", false));
		}
		return job;
	}

	private static void appendNewFiles(Path currentPath, OperationsParams params)
			throws IOException, InterruptedException {
		// Read master file to get all file names
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> insertPartitions = new ArrayList<Partition>();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");

		Path currentMasterPath = new Path(currentPath, "_master." + sindex);
		Text tempLine = new Text2();
		LineReader in = new LineReader(fs.open(currentMasterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			currentPartitions.add(tempPartition);
		}

		Path insertMasterPath = new Path(currentPath, "temp/_master." + sindex);
		in = new LineReader(fs.open(insertMasterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			insertPartitions.add(tempPartition);
		}

		for (Partition currentPartition : currentPartitions) {
			for (Partition insertPartition : insertPartitions) {
				if (currentPartition.cellId == insertPartition.cellId) {
					currentPartition.expand(insertPartition);
				}
			}
		}

		// Append files in temp directory to corresponding files in current path
		for (Partition partition : currentPartitions) {
			System.out.println("Appending to " + partition.filename);
			FSDataOutputStream out = fs.append(new Path(currentPath, partition.filename));
			for (Partition insertPartition : insertPartitions) {
				if (partition.cellId == insertPartition.cellId) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(new Path(currentPath, "temp/" + insertPartition.filename))));
					String line;
					PrintWriter writer = new PrintWriter(out);
					do {
						line = br.readLine();
						if (line != null) {
							writer.append("\n" + line);
						}
					} while (line != null);
					writer.close();
				}
			}
			out.close();
		}

		// Update master and wkt file
		Path currentWKTPath = new Path(currentPath, "_" + sindex + ".wkt");
		fs.delete(currentWKTPath);
		fs.delete(currentMasterPath);
		PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
		wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		OutputStream masterOut = fs.create(currentMasterPath);
		for (Partition currentPartition : currentPartitions) {
			Text masterLine = new Text2();
			currentPartition.toText(masterLine);
			masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
			masterOut.write(NewLine);
			wktOut.println(currentPartition.toWKT());
		}

		wktOut.close();
		masterOut.close();
		fs.delete(new Path(currentPath, "temp"));
		// fs.close();
		System.out.println("Complete!");
	}

	// Reorganize index with R-Tree splitting mechanism
	private static void reorganizeWithRTreeSplitter(Path path, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> reorganizedPartitions = new ArrayList<Partition>();

		Job job = new Job(params, "Reorganizer");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");

		Path currentMasterPath = new Path(path, "_master." + sindex);
		Text tempLine1 = new Text2();
		LineReader in1 = new LineReader(fs.open(currentMasterPath));
		while (in1.readLine(tempLine1) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine1);
			currentPartitions.add(tempPartition);
		}

		int maxCellId = -1;
		for (Partition partition : currentPartitions) {
			if (partition.cellId > maxCellId) {
				maxCellId = partition.cellId;
			}
		}

		// Iterate all overflow partitions to split
		for (Partition partition : currentPartitions) {
			reorganizedPartitions.add(partition);
			if (partition.size >= overflowSize) {
				System.out.println("Overflow--------------------");
				Path tempOutputPath = new Path("./", "temp.output");
				OperationsParams params2 = new OperationsParams(conf);
				Rectangle inputMBR = FileMBR.fileMBR(new Path(path, partition.filename), new OperationsParams(conf));
				params2.setShape(conf, "mbr", inputMBR);
				params2.setBoolean("local", false);
				Indexer.index(new Path(path, partition.filename), tempOutputPath, params2);
				reorganizedPartitions.remove(partition);
				fs.delete(new Path(path, partition.filename));
				LineReader in = new LineReader(fs.open(new Path(tempOutputPath, "_master." + sindex)));
				Text tempLine = new Text2();
				while (in.readLine(tempLine) > 0) {
					Partition tempPartition = new Partition();
					tempPartition.fromText(tempLine);
					maxCellId++;
					tempPartition.cellId = maxCellId;
					String oldFileName = tempPartition.filename;
					tempPartition.filename = String.format("part-%05d", tempPartition.cellId);
					fs.rename(new Path(tempOutputPath, oldFileName), new Path(path, tempPartition.filename));
					reorganizedPartitions.add(tempPartition);
				}
				fs.delete(tempOutputPath);
			}
		}
		// Update master and wkt file
		currentMasterPath = new Path(path, "_master." + sindex);
		Path currentWKTPath = new Path(path, "_" + sindex + ".wkt");
		fs.delete(currentMasterPath);
		fs.delete(currentWKTPath);
		OutputStream masterOut = fs.create(currentMasterPath);
		PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
		wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		for (Partition partition : reorganizedPartitions) {
			Text masterLine = new Text2();
			partition.toText(masterLine);
			masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
			masterOut.write(NewLine);
			wktOut.println(partition.toWKT());
		}

		wktOut.close();
		masterOut.close();
	}

	private static void greedyReorganizeWithRTreeSplitter(Path path, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splittingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> reorganizedPartitions = new ArrayList<Partition>();

		Job job = new Job(params, "Reorganizer");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");
		double budget = Double.parseDouble(params.get("budget")) * 1024 * 1024;

		// Load current partitions
		Path currentMasterPath = new Path(path, "_master." + sindex);
		Text tempLine1 = new Text2();
		LineReader in1 = new LineReader(fs.open(currentMasterPath));
		while (in1.readLine(tempLine1) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine1);
			currentPartitions.add(tempPartition);
		}

		int maxCellId = -1;
		for (Partition partition : currentPartitions) {
			if (partition.cellId > maxCellId) {
				maxCellId = partition.cellId;
			}
		}

		// Map partitions to graph then find the dense clusters
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(currentPartitions);
		Set<Node> selectedNodes = GraphUtils.findMaximalMultipleWeightedSubgraphs2(graph, budget);
		for (int i = 0; i < currentPartitions.size(); i++) {
			for (Node node : selectedNodes) {
				if (node.getLabel().equals(Integer.toString(i))) {
					splittingPartitions.add(currentPartitions.get(i));
				}
			}
		}

		// Split selected partitions
		// Iterate all overflow partitions to split
		for (Partition partition : currentPartitions) {
			reorganizedPartitions.add(partition);
			if (splittingPartitions.contains(partition) && partition.size >= overflowSize) {
				System.out.println("Splitting--------------------");
				Path tempOutputPath = new Path("./", "temp.output");
				OperationsParams params2 = new OperationsParams(conf);
				Rectangle inputMBR = FileMBR.fileMBR(new Path(path, partition.filename), new OperationsParams(conf));
				params2.setShape(conf, "mbr", inputMBR);
				params2.setBoolean("local", false);
				Indexer.index(new Path(path, partition.filename), tempOutputPath, params2);
				reorganizedPartitions.remove(partition);
				fs.delete(new Path(path, partition.filename));
				LineReader in = new LineReader(fs.open(new Path(tempOutputPath, "_master." + sindex)));
				Text tempLine = new Text2();
				while (in.readLine(tempLine) > 0) {
					Partition tempPartition = new Partition();
					tempPartition.fromText(tempLine);
					maxCellId++;
					tempPartition.cellId = maxCellId;
					String oldFileName = tempPartition.filename;
					tempPartition.filename = String.format("part-%05d", tempPartition.cellId);
					fs.rename(new Path(tempOutputPath, oldFileName), new Path(path, tempPartition.filename));
					reorganizedPartitions.add(tempPartition);
				}
				fs.delete(tempOutputPath);
			}
		}
		
		// Update master and wkt file
		currentMasterPath = new Path(path, "_master." + sindex);
		Path currentWKTPath = new Path(path, "_" + sindex + ".wkt");
		fs.delete(currentMasterPath);
		fs.delete(currentWKTPath);
		OutputStream masterOut = fs.create(currentMasterPath);
		PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
		wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		for (Partition partition : reorganizedPartitions) {
			Text masterLine = new Text2();
			partition.toText(masterLine);
			masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
			masterOut.write(NewLine);
			wktOut.println(partition.toWKT());
		}

		wktOut.close();
		masterOut.close();
	}

	private static void printUsage() {
		System.out.println("Incrementally index data with R-Tree splitting mechanism");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		// Path[] inputFiles = params.getPaths();

		// if (!params.checkInput() || (inputFiles.length != 2)) {
		// printUsage();
		// System.exit(1);
		// }

		// Path currentPath = inputFiles[0];
		// Path appendPath = inputFiles[1];

		String currentPathString = params.get("current");
		String appendPathString = params.get("append");
		Path currentPath = new Path(currentPathString);
		Path appendPath = new Path(appendPathString);

		System.out.println("Current index path: " + currentPath);
		System.out.println("Append data path: " + appendPath);

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		// Append new data to old data
		insertMapReduce(currentPath, appendPath, params);
		appendNewFiles(currentPath, params);
		long t2 = System.currentTimeMillis();
//		reorganizeWithRTreeSplitter(currentPath, params);
		greedyReorganizeWithRTreeSplitter(currentPath, params);
		long t3 = System.currentTimeMillis();
		System.out.println("Total appending time in millis " + (t2 - t1));
		System.out.println("Total repartitioning time in millis " + (t3 - t2));
		System.out.println("Total time in millis " + (t3 - t1));
	}
}
