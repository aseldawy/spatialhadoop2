package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.Inserter.InserterMap;
import edu.umn.cs.spatialHadoop.indexing.Inserter.InserterReduce;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class RTreeIndexer {
	
	private static final Log LOG = LogFactory.getLog(Indexer.class);
	private static final Map<String, Class<? extends LocalIndexer>> LocalIndexes;

	static {
		LocalIndexes = new HashMap<String, Class<? extends LocalIndexer>>();
		LocalIndexes.put("rtree", RTreeLocalIndexer.class);
		LocalIndexes.put("r+tree", RTreeLocalIndexer.class);
	}
	
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
				context.write(partitionID, shape);
				context.progress();
			}
			// Indicate end of partition to close the file
			context.write(new IntWritable(-partitionID.get() - 1), null);
			LOG.info("Done with partition #" + partitionID);
		}
	}

	private static Job insertMapReduce(Path currentPath, Path insertPath, OperationsParams params) throws IOException,
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

		// Load the partitioner from file
		String index = conf.get("sindex");
		if (index == null)
			throw new RuntimeException("Index type is not set");
		setLocalIndexer(conf, index);
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
		SpatialInputFormat3.setInputPaths(job, insertPath);
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
	
	private static void appendNewFiles(Path currentPath, OperationsParams params) throws IOException, InterruptedException {
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
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fs.open(new Path(currentPath, "temp/" + partition.filename))));
			String line;
			PrintWriter writer = new PrintWriter(out);
			do {
				line = br.readLine();
				if (line != null) {
					writer.append("\n" + line);
				}
			} while (line != null);
			writer.close();
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
		fs.close();
		System.out.println("Complete!");
	}

	// Read the input by buffer then put them to a temporary input
	private static void readInputByBuffer(Path inputPath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
		String line;
		line = br.readLine();
		int n = 0;
		while (line != null) {
			System.out.println(line);
			n++;
			if(n == 2) {
				break;
			}
			line = br.readLine();
		}
	}
	
	private static void createEmptyPartition(Path outputPath, OperationsParams params) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		String mbr = params.get("mbr");
		mbr = mbr.trim();
		String emptyPartitionName = "part-00000";
		System.out.println("mbr: " + mbr);
		fs.create(new Path(outputPath, emptyPartitionName), true);
		fs.create(new Path(outputPath, "_rtree.wkt"), true);
		FSDataOutputStream os = fs.create(new Path(outputPath, "_master.rtree"), true);
		Text emptyPartitionMetadata = new Text2("0," + mbr + ",0,0," + emptyPartitionName);
		os.write(emptyPartitionMetadata.getBytes(), 0, emptyPartitionMetadata.getLength());
		fs.close();
	}
	
	/**
	 * Set the local indexer for the given job configuration.
	 * 
	 * @param job
	 * @param sindex
	 */
	private static void setLocalIndexer(Configuration conf, String sindex) {
		Class<? extends LocalIndexer> localIndexerClass = LocalIndexes.get(sindex);
		if (localIndexerClass != null)
			conf.setClass(LocalIndexer.LocalIndexerClass, localIndexerClass, LocalIndexer.class);
	}

	private static void printUsage() {
		System.out.println("Index data using R-Tree splitting mechanism");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		if (!params.checkInputOutput(true)) {
			printUsage();
			return;
		}

		Path inputPath = params.getInputPath();
		Path outputPath = params.getOutputPath();

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		System.out.println("Input path: " + inputPath);
		System.out.println("Output path: " + outputPath);
		createEmptyPartition(outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis " + (t2 - t1));
	}
}
