package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import edu.umn.cs.spatialHadoop.indexing.Indexer.PartitionerMap;
import edu.umn.cs.spatialHadoop.indexing.Indexer.PartitionerReduce;
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
	 * The map function that partitions the data using the configured partitioner.
	 * Refer from Indexer class
	 * 
	 * @author Tin Vu
	 *
	 */
	public static class InserterMap extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private Partitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to assign it
		 * to only one partition
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
			Partition p = (Partition) key;
			LOG.info("map partition = " + p.cellId + " and filename: " + p.filename);
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
					// LOG.info("overlapPartition");
					partitionID.set(partitioner.overlapPartition(shape));
					if (partitionID.get() >= 0)
						context.write(partitionID, shape);
				}
				context.progress();
			}
		}
	}

	/**
	 * The map function that partitions the data using the configured partitioner
	 * 
	 * @author Eldawy
	 *
	 */
	public static class IncrementalPartitionerMap
			extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private IncrementalRTreeFilePartitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to assign it
		 * to only one partition
		 */
		private boolean replicate;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.partitioner = (IncrementalRTreeFilePartitioner) Partitioner.getPartitioner(context.getConfiguration());
			this.replicate = context.getConfiguration().getBoolean("replicate", false);
		}

		@Override
		protected void map(Rectangle key, Iterable<? extends Shape> shapes, final Context context)
				throws IOException, InterruptedException {
			Partition p = (Partition) key;
			LOG.info("map partition = " + p.cellId + " and filename: " + p.filename);
			final IntWritable partitionID = new IntWritable();
			for (final Shape shape : shapes) {
				if (replicate) {
					partitioner.overlapPartitions(p.filename, shape, new ResultCollector<Integer>() {
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
					partitionID.set(partitioner.overlapPartition(p.filename, shape));
					if (partitionID.get() >= 0) {
						context.write(partitionID, shape);
					} else {
						LOG.info("no partition ID");
					}
				}
				context.progress();
			}
		}
	}

	public static class IncrementalPartitionerReduce<S extends Shape>
			extends Reducer<IntWritable, Shape, IntWritable, Shape> {

		@Override
		protected void reduce(IntWritable partitionID, Iterable<Shape> shapes, Context context)
				throws IOException, InterruptedException {
			LOG.info("Working on partition #" + partitionID);
			for (Shape shape : shapes) {
				if (shape != null) {
					context.write(partitionID, shape);
					context.progress();
				} else {
					LOG.info("Shape is null");
				}
			}
			// Indicate end of partition to close the file
			context.write(new IntWritable(-partitionID.get() - 1), null);
			LOG.info("Done with partition #" + partitionID);
		}
	}

	public static class GreedyRTreePartitionerMap
			extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private GreedyRTreePartitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to assign it
		 * to only one partition
		 */
		private boolean replicate;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.partitioner = (GreedyRTreePartitioner) Partitioner.getPartitioner(context.getConfiguration());
			this.replicate = context.getConfiguration().getBoolean("replicate", false);
		}

		@Override
		protected void map(Rectangle key, Iterable<? extends Shape> shapes, final Context context)
				throws IOException, InterruptedException {
			Partition p = (Partition) key;
			LOG.info("map partition = " + p.cellId + " and filename: " + p.filename);
			final IntWritable partitionID = new IntWritable();
			for (final Shape shape : shapes) {
				if (replicate) {
					partitioner.overlapPartitions(p.filename, shape, new ResultCollector<Integer>() {
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
					partitionID.set(partitioner.overlapPartition(p.filename, shape));
					if (partitionID.get() >= 0) {
						context.write(partitionID, shape);
					} else {
						LOG.info("no partition ID");
					}
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

	public static class GreedyRTreePartitionerReduce<S extends Shape>
			extends Reducer<IntWritable, Shape, IntWritable, Shape> {

		@Override
		protected void reduce(IntWritable partitionID, Iterable<Shape> shapes, Context context)
				throws IOException, InterruptedException {
			LOG.info("Working on partition #" + partitionID);
			for (Shape shape : shapes) {
				if (shape != null) {
					context.write(partitionID, shape);
					context.progress();
				} else {
					LOG.info("Shape is null");
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
			// System.out.println("Appending to " + partition.filename);
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
				// System.out.println("Overflow--------------------");
				Path tempOutputPath = new Path("./", "temp.output");
				OperationsParams params2 = new OperationsParams(conf);
				Rectangle inputMBR = FileMBR.fileMBR(new Path(path, partition.filename), new OperationsParams(conf));
				params2.setShape(conf, "mbr", inputMBR);
				params2.setBoolean("local", false);
				Indexer.indexLocal(new Path(path, partition.filename), tempOutputPath, params2);
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

	private static Job reorganizeWithRTreeSplitter2(Path path, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> reorganizedPartitions = new ArrayList<Partition>();

		Job job = new Job(params, "Reorganizer");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		double blockSize = Double.parseDouble(conf.get("dfs.blocksize"));
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
		Path tempInputPath = new Path("./", "temp.incrtree.input");
		Path tempOutputPath = new Path("./", "temp.incrtree.output");

		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);
		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		for (Partition partition : currentPartitions) {
			reorganizedPartitions.add(partition);
			if (partition.size >= overflowSize) {
				splitPartitions.add(partition);
				totalSplitSize += partition.size;
				totalSplitBlocks += partition.getNumberOfBlock(blockSize);
				reorganizedPartitions.remove(partition);
				FileUtil.copy(fs, new Path(path, partition.filename), fs, new Path(tempInputPath, partition.filename),
						false, true, conf);
			}
		}

		System.out.println("Total split partitions = " + splitPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splitPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splitPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		IncrementalRTreeFilePartitioner partitioner = new IncrementalRTreeFilePartitioner();
		partitioner.createFromInputFile(path, tempOutputPath, params);
		// for (Partition p : partitioner.cells) {
		// System.out.println("cell info = " + p.toString());
		// }
		Partitioner.setPartitioner(conf, partitioner);

		// Set mapper and reducer
		Shape shape = OperationsParams.getShape(conf, "shape");
		job.setMapperClass(IncrementalPartitionerMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setReducerClass(IncrementalPartitionerReduce.class);
		// Set input and output
		job.setInputFormatClass(SpatialInputFormat3.class);
		SpatialInputFormat3.setInputPaths(job, tempInputPath);
		job.setOutputFormatClass(IndexOutputFormat.class);
		IndexOutputFormat.setOutputPath(job, tempOutputPath);
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

		// Merge
		ArrayList<Partition> tempPartitions = new ArrayList<Partition>();
		Path tempMasterPath = new Path(tempOutputPath, "_master." + sindex);
		Text tempLine = new Text2();
		LineReader in = new LineReader(fs.open(tempMasterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			tempPartitions.add(tempPartition);
		}

		for (Partition p : tempPartitions) {
			fs.rename(new Path(tempOutputPath, p.filename), new Path(path, p.filename));
		}

		reorganizedPartitions.addAll(tempPartitions);

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

		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		for (Partition p : splitPartitions) {
			fs.delete(new Path(path, p.filename));
		}

		return job;
	}

	private static Job greedyReorganizeWithRTreeSplitter(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splittingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> reorganizedPartitions = new ArrayList<Partition>();

		Job job = new Job(params, "Reorganizer");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
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
		ArrayList<ArrayList<Partition>> groups = new ArrayList<ArrayList<Partition>>();

		ArrayList<Partition> oneBlockPartitions = GreedyRepartitioner2.getOneBlockPartitions(currentPartitions,
				blockSize);
		System.out.println("Number of one block partitions = " + oneBlockPartitions.size());
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(oneBlockPartitions);
		System.out.println("Number of node in mapped graph = " + graph.vertexSet().size());
		ArrayList<Set<Node>> selectedNodeSets = GraphUtils.findMaximalMultipleWeightedSubgraphs3(graph, budget);
		System.out.println("Number of selected groups = " + selectedNodeSets.size());

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;

		for (Set<Node> nodeSet : selectedNodeSets) {
			System.out.println("Set of node");
			for (Node node : nodeSet) {
				System.out.println("Node label = " + node.getLabel());
			}
			ArrayList<Partition> group = new ArrayList<Partition>();
			for (int i = 0; i < currentPartitions.size(); i++) {
				for (Node node : nodeSet) {
					if (node.getLabel().equals(Integer.toString(i))) {
						if (!isContainedIn(currentPartitions.get(i), group)) {
							group.add(currentPartitions.get(i));
							splittingPartitions.add(currentPartitions.get(i));
							totalSplitSize += currentPartitions.get(i).size;
							totalSplitBlocks += currentPartitions.get(i).getNumberOfBlock(blockSize);
						}
					}
				}
			}
			groups.add(group);
		}

		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		int groupId = 0;
		ArrayList<RTreePartitioner> partitioners = new ArrayList<RTreePartitioner>();
		for (ArrayList<Partition> group : groups) {
			groupId++;
			Path groupInputPath = new Path("./", "temp.greedy.input" + groupId);
			Path groupOutputPath = new Path("./", "temp.greedy.output" + groupId);

			// Move all partitions of this group to temporary input
			if (fs.exists(groupInputPath)) {
				fs.delete(groupInputPath);
			}
			fs.mkdirs(groupInputPath);

			if (fs.exists(groupOutputPath)) {
				fs.delete(groupOutputPath);
			}

			Rectangle inputMBR = new Rectangle(group.get(0));
			for (Partition p : group) {
				fs.rename(new Path(path, p.filename), new Path(groupInputPath, p.filename));
				inputMBR.expand(p);
			}

			OperationsParams params2 = new OperationsParams(conf);
			params2.setShape(conf, "mbr", inputMBR);
			params2.setBoolean("local", false);

			// Indexer.index(tempInputPath, tempOutputPath, params2);
			Job job2 = new Job(params2, "Sampler");
			Configuration conf2 = job2.getConfiguration();
			RTreePartitioner groupPartitioner = (RTreePartitioner) Indexer.createPartitioner(groupInputPath,
					groupOutputPath, conf2, sindex);
			partitioners.add(groupPartitioner);

			// LineReader in = new LineReader(fs.open(new Path(tempOutputPath,
			// "_master." + sindex)));
			// Text tempLine = new Text2();
			// while (in.readLine(tempLine) > 0) {
			// Partition tempPartition = new Partition();
			// tempPartition.fromText(tempLine);
			// maxCellId++;
			// tempPartition.cellId = maxCellId;
			// String oldFileName = tempPartition.filename;
			// tempPartition.filename = String.format("part-%05d",
			// tempPartition.cellId);
			// fs.rename(new Path(tempOutputPath, oldFileName), new Path(path,
			// tempPartition.filename));
			// reorganizedPartitions.add(tempPartition);
			// }
			// fs.delete(tempInputPath);
			// fs.delete(tempOutputPath);
		}

		currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

		GreedyRTreePartitioner greedyPartitioner = new GreedyRTreePartitioner(partitioners, maxCellId);
		Partitioner.setPartitioner(conf, greedyPartitioner);

		// Set mapper and reducer
		Path[] tempInputPaths = new Path[groupId];
		Path tempOutputPath = new Path("./", "temp.greedy.output");
		for (int i = 0; i < groupId; i++) {
			Path groupInputPath = new Path("./", "temp.greedy.input" + (i + 1));
			tempInputPaths[i] = groupInputPath;
		}

		Shape shape = OperationsParams.getShape(conf, "shape");
		job.setMapperClass(GreedyRTreePartitionerMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setReducerClass(GreedyRTreePartitionerReduce.class);
		// Set input and output
		job.setInputFormatClass(SpatialInputFormat3.class);
		SpatialInputFormat3.setInputPaths(job, tempInputPaths);
		job.setOutputFormatClass(IndexOutputFormat.class);
		IndexOutputFormat.setOutputPath(job, tempOutputPath);
		// Set number of reduce tasks according to cluster status
		ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
		job.setNumReduceTasks(Math.max(1,
				Math.min(greedyPartitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));

		// Use multithreading in case the job is running locally
		conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

		// Start the job
		if (conf.getBoolean("background", false)) {
			// Run in background
			job.submit();
		} else {
			job.waitForCompletion(conf.getBoolean("verbose", false));
		}

		// Merge
		ArrayList<Partition> tempPartitions = new ArrayList<Partition>();
		Path tempMasterPath = new Path(tempOutputPath, "_master." + sindex);
		Text tempLine = new Text2();
		LineReader in = new LineReader(fs.open(tempMasterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			tempPartitions.add(tempPartition);
		}

		for (Partition p : tempPartitions) {
			fs.rename(new Path(tempOutputPath, p.filename), new Path(path, p.filename));
		}

		reorganizedPartitions.addAll(tempPartitions);

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

		for (int i = 1; i <= groupId; i++) {
			Path groupInputPath = new Path("./", "temp.greedy.input" + i);
			fs.delete(groupInputPath);
		}
		fs.delete(tempOutputPath);

		for (Partition p : splittingPartitions) {
			fs.delete(new Path(path, p.filename));
		}

		return job;

		// // Update master and wkt file
		// currentMasterPath = new Path(path, "_master." + sindex);
		// Path currentWKTPath = new Path(path, "_" + sindex + ".wkt");
		// fs.delete(currentMasterPath);
		// fs.delete(currentWKTPath);
		// OutputStream masterOut = fs.create(currentMasterPath);
		// PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
		// wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		// for (Partition partition : reorganizedPartitions) {
		// Text masterLine = new Text2();
		// partition.toText(masterLine);
		// masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
		// masterOut.write(NewLine);
		// wktOut.println(partition.toWKT());
		// }
		//
		// wktOut.close();
		// masterOut.close();
	}

	private static void greedyReorganizeWithRTreeSplitter2(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
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
		ArrayList<Partition> oneBlockPartitions = GreedyRepartitioner2.getOneBlockPartitions(currentPartitions,
				blockSize);
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(oneBlockPartitions);
		Set<Node> selectedNodes = GraphUtils.findMaximalWeightedSubgraph2(graph, budget);

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		for (int i = 0; i < currentPartitions.size(); i++) {
			for (Node node : selectedNodes) {
				if (node.getLabel().equals(Integer.toString(i))) {
					if (!isContainedIn(currentPartitions.get(i), splittingPartitions)) {
						splittingPartitions.add(currentPartitions.get(i));
						totalSplitSize += currentPartitions.get(i).size;
						totalSplitBlocks += currentPartitions.get(i).getNumberOfBlock(blockSize);
					}
				}
			}
		}

		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.greedy2.input");
		Path tempOutputPath = new Path("./", "temp.greedy2.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splittingPartitions.get(0));
		for (Partition p : splittingPartitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		params2.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);

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
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		// currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

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

	private static void greedyReorganizeWithAreaSizeOptimizer(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
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
		ArrayList<Partition> oneBlockPartitions = GreedyRepartitioner2.getOneBlockPartitions(currentPartitions,
				blockSize);
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(oneBlockPartitions);
		Set<Node> selectedNodes = GraphUtils.findMaximalWeightedSubgraph2(graph, budget);

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		for (int i = 0; i < currentPartitions.size(); i++) {
			for (Node node : selectedNodes) {
				if (node.getLabel().equals(Integer.toString(i))) {
					if (!isContainedIn(currentPartitions.get(i), splittingPartitions)) {
						splittingPartitions.add(currentPartitions.get(i));
						totalSplitSize += currentPartitions.get(i).size;
						totalSplitBlocks += currentPartitions.get(i).getNumberOfBlock(blockSize);
					}
				}
			}
		}

		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.greedy2.input");
		Path tempOutputPath = new Path("./", "temp.greedy2.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splittingPartitions.get(0));
		for (Partition p : splittingPartitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		params2.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);

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
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		// currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

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

	private static void greedyReorganizeWithAreaOptimizer(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
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
		ArrayList<Partition> oneBlockPartitions = GreedyRepartitioner2.getOneBlockPartitions(currentPartitions,
				blockSize);
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(oneBlockPartitions);
		Set<Node> selectedNodes = GraphUtils.findMaximalWeightedSubgraph2(graph, budget);

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		for (int i = 0; i < currentPartitions.size(); i++) {
			for (Node node : selectedNodes) {
				if (node.getLabel().equals(Integer.toString(i))) {
					if (!isContainedIn(currentPartitions.get(i), splittingPartitions)) {
						splittingPartitions.add(currentPartitions.get(i));
						totalSplitSize += currentPartitions.get(i).size;
						totalSplitBlocks += currentPartitions.get(i).getNumberOfBlock(blockSize);
					}
				}
			}
		}

		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.greedy2.input");
		Path tempOutputPath = new Path("./", "temp.greedy2.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splittingPartitions.get(0));
		for (Partition p : splittingPartitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		params2.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);

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
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		// currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

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

	private static void greedyReorganizeWithOverlappingOptimizer(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
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
		ArrayList<Partition> oneBlockPartitions = GreedyRepartitioner2.getOneBlockPartitions(currentPartitions,
				blockSize);
		SimpleWeightedGraph<Node, DefaultWeightedEdge> graph = GreedyRepartitioner2
				.mapPartitionsToGraph(oneBlockPartitions);
		Set<Node> selectedNodes = GraphUtils.findMaximalWeightedSubgraph2(graph, budget);

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		for (int i = 0; i < currentPartitions.size(); i++) {
			for (Node node : selectedNodes) {
				if (node.getLabel().equals(Integer.toString(i))) {
					if (!isContainedIn(currentPartitions.get(i), splittingPartitions)) {
						splittingPartitions.add(currentPartitions.get(i));
						totalSplitSize += currentPartitions.get(i).size;
						totalSplitBlocks += currentPartitions.get(i).getNumberOfBlock(blockSize);
					}
				}
			}
		}

		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.greedy2.input");
		Path tempOutputPath = new Path("./", "temp.greedy2.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splittingPartitions.get(0));
		for (Partition p : splittingPartitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		params2.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);

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
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		// currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

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

	private static void optimalReorganizeWithMaximumReducedCost(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
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

		Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (inputMBR == null) {
			inputMBR = new Rectangle(currentPartitions.get(0));
			for (Partition p : splittingPartitions) {
				inputMBR.expand(p);
			}
		}
		double querySize = 0.000001 * Math.sqrt(inputMBR.getSize());
		System.out.println("Query size = " + querySize);

		computeReducedCost(currentPartitions, querySize, blockSize);
	}

	@SuppressWarnings("unchecked")
	private static void greedyReorganizeWithMaximumReducedCost(Path path, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splittingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> remainingPartitions = new ArrayList<Partition>();
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

		remainingPartitions = (ArrayList<Partition>) currentPartitions.clone();

		int maxCellId = -1;
		for (Partition partition : currentPartitions) {
			if (partition.cellId > maxCellId) {
				maxCellId = partition.cellId;
			}
		}

		Rectangle mbr = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (mbr == null) {
			mbr = new Rectangle(currentPartitions.get(0));
			for (Partition p : splittingPartitions) {
				mbr.expand(p);
			}
		}
		double querySize = 0.000001 * Math.sqrt(mbr.getSize());
		System.out.println("Query size = " + querySize);

		int budgetBlocks = (int) Math.ceil(budget / blockSize);

		// Find the partition with maximum reduced cost as a seed for our greedy
		// algorithm
		Partition maxReducedCostPartition = currentPartitions.get(0);
		double maxReducedCost = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double pReducedCost = computeReducedCost(splittingPartitions, querySize, blockSize);
			if (maxReducedCost < pReducedCost) {
				maxReducedCost = pReducedCost;
				maxReducedCostPartition = p;
			}
			splittingPartitions.remove(p);
		}

		splittingPartitions.add(maxReducedCostPartition);
		remainingPartitions.remove(maxReducedCostPartition);
		budgetBlocks -= maxReducedCostPartition.getNumberOfBlock(blockSize);

		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceCost(remainingPartitions, splittingPartitions,
					querySize, blockSize);
			splittingPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}

		// Now splitting partition is the list of partition that promise the biggest
		// reduced cost
		ArrayList<ArrayList<Partition>> groups = new ArrayList<ArrayList<Partition>>();
		@SuppressWarnings("unchecked")
		ArrayList<Partition> tempSplittingPartitions = (ArrayList<Partition>) splittingPartitions.clone();

		long totalSplitSize = 0;
		int totalSplitBlocks = 0;
		while (tempSplittingPartitions.size() > 0) {
			ArrayList<Partition> group = new ArrayList<Partition>();
			group.add(tempSplittingPartitions.get(0));
			for (Partition p : tempSplittingPartitions) {
				if (isOverlapping(p, group)) {
					group.add(p);
				}
			}
			groups.add(group);
			tempSplittingPartitions.removeAll(group);
		}

		for (Partition p : splittingPartitions) {
			totalSplitSize += p.size;
			totalSplitBlocks += p.getNumberOfBlock(blockSize);
		}

		System.out.println("Number of groups = " + groups.size());
		System.out.println("Total split partitions = " + splittingPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		// Save split partitions and keep partitions to file
		Path splitPath = new Path(path, "rects.split");
		Path keepPath = new Path(path, "rects.keep");
		OutputStream splitOut = fs.create(splitPath);
		OutputStream keepOut = fs.create(keepPath);
		for (Partition partition : splittingPartitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			splitOut.write(splitLine.getBytes(), 0, splitLine.getLength());
			splitOut.write(NewLine);
		}
		currentPartitions.removeAll(splittingPartitions);
		for (Partition partition : currentPartitions) {
			Text keepLine = new Text2();
			partition.toText(keepLine);
			keepOut.write(keepLine.getBytes(), 0, keepLine.getLength());
			keepOut.write(NewLine);
		}
		splitOut.close();
		keepOut.close();

		// Split selected partitions
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.greedyreducedcost.input");
		Path tempOutputPath = new Path("./", "temp.greedyreducedcost.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splittingPartitions.get(0));
		for (Partition p : splittingPartitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		params2.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);

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
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		// currentPartitions.removeAll(splittingPartitions);
		reorganizedPartitions.addAll(currentPartitions);

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

	private static Partition findBestCandidateToReduceCost(ArrayList<Partition> currentPartitions,
			ArrayList<Partition> splittingPartitions, double querySize, int blockSize) {
		Partition bestPartition = currentPartitions.get(0);
		double maxReducedCost = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double splittingReducedCost = computeReducedCost(splittingPartitions, querySize, blockSize);
			if (maxReducedCost < splittingReducedCost) {
				bestPartition = p;
			}
			splittingPartitions.remove(p);
		}

		return bestPartition;
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
				if (isOverlapping(p, group)) {
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

	private ArrayList<Partition> getPartitionWithMaximumReducedCost(ArrayList<Partition> partitions, int budget) {
		ArrayList<Partition> splittingPartitions = new ArrayList<Partition>();

		return splittingPartitions;
	}

	private static boolean isContainedIn(Partition p, ArrayList<Partition> partitions) {
		for (Partition partition : partitions) {
			if (p.cellId == partition.cellId) {
				return true;
			}
		}

		return false;
	}

	private static boolean isOverlapping(Partition p, ArrayList<Partition> partitions) {
		for (Partition partition : partitions) {
			if ((p.cellId != partition.cellId) && p.isIntersected(partition)) {
				return true;
			}
		}

		return false;
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

//		Path[] inputFiles = params.getPaths();

//		if (!params.checkInput() || (inputFiles.length != 2)) {
//			printUsage();
//			System.exit(1);
//		}

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
		System.out.println("Total appending time in millis " + (t2 - t1));

		String splitType = params.get("splittype");
		if (splitType.equals("rtree2")) {
			System.out.println("R-Tree splitting");
			Job job = reorganizeWithRTreeSplitter2(currentPath, params);
		} else if (splitType.equals("greedy")) {
			System.out.println("Greedy splitting: multiple cluster");
			greedyReorganizeWithRTreeSplitter(currentPath, params);
		} else if (splitType.equals("greedysingle")) {
			System.out.println("Greedy splitting: single cluster");
			greedyReorganizeWithRTreeSplitter2(currentPath, params);
		} else if (splitType.equals("areasize")) {
			System.out.println("Greedy splitting: optimize area*size");
			greedyReorganizeWithAreaSizeOptimizer(currentPath, params);
		} else if (splitType.equals("area")) {
			System.out.println("Greedy splitting: optimize area");
			greedyReorganizeWithAreaOptimizer(currentPath, params);
		} else if (splitType.equals("overlapping")) {
			System.out.println("Greedy splitting: optimize overlapping");
			greedyReorganizeWithOverlappingOptimizer(currentPath, params);
		} else if (splitType.equals("optimal")) {
			System.out.println("Optimal splitting: maximize the reduced cost of range queries");
			optimalReorganizeWithMaximumReducedCost(currentPath, params);
		} else if (splitType.equals("greedyreducedcost")) {
			System.out.println("Greedy splitting: maximize the reduced cost of range queries");
			greedyReorganizeWithMaximumReducedCost(currentPath, params);
		}

		long t3 = System.currentTimeMillis();
		System.out.println("Total repartitioning time in millis " + (t3 - t2));
		System.out.println("Total time in millis " + (t3 - t1));
	}
}
