package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

public class RTreeReorganizer {

	private static final Log LOG = LogFactory.getLog(Indexer.class);

	public static void reorganizeGroups(Path path, ArrayList<ArrayList<Partition>> splitGroups, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		// Indexing each group separately
		for(ArrayList<Partition> group: splitGroups) {
			reorganizeSingleGroup(path, group, params);
		}
	}

	public static void reorganizeSingleGroup(Path path, ArrayList<Partition> splitPatitions, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(params, "ReorganizeGroup");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");
		
		// Find max id
		ArrayList<Partition> currentPartitions = MetadataUtil.getPartitions(path, params);
		int maxCellId = MetadataUtil.getMaximumCellId(currentPartitions);

		// Indexing all partitions in the group
		// Iterate all overflow partitions to split
		Path tempInputPath = new Path("./", "temp.ReorganizeSingleGroup.input");
		Path tempOutputPath = new Path("./", "temp.ReorganizeSingleGroup.output");

		// Move all partitions of this group to temporary input
		if (fs.exists(tempInputPath)) {
			fs.delete(tempInputPath);
		}
		fs.mkdirs(tempInputPath);

		if (fs.exists(tempOutputPath)) {
			fs.delete(tempOutputPath);
		}

		Rectangle inputMBR = new Rectangle(splitPatitions.get(0));
		for (Partition p : splitPatitions) {
			fs.rename(new Path(path, p.filename), new Path(tempInputPath, p.filename));
			inputMBR.expand(p);
		}

		OperationsParams params2 = new OperationsParams(conf);
		OperationsParams.setShape(conf, "mbr", inputMBR);
		params2.setBoolean("local", false);

		Indexer.index(tempInputPath, tempOutputPath, params2);
		
		// Merge
		currentPartitions = MetadataUtil.removePartitions(currentPartitions, splitPatitions);
		ArrayList<Partition> tempPartitions = MetadataUtil.getPartitions(tempOutputPath, params2);
		for(Partition tempPartition: tempPartitions) {
			maxCellId++;
			tempPartition.cellId = maxCellId;
			String oldFileName = tempPartition.filename;
			tempPartition.filename = String.format("part-%05d", tempPartition.cellId);
			fs.rename(new Path(tempOutputPath, oldFileName), new Path(path, tempPartition.filename));
			currentPartitions.add(tempPartition);
		}
		
		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);
		
		// Update master and wkt file
		Path currentMasterPath = new Path(path, "_master." + sindex);
		fs.delete(currentMasterPath);
		MetadataUtil.dumpToFile(currentPartitions, currentMasterPath);
		Path currentWKTPath = new Path(path, "_" + sindex + ".wkt");
		MetadataUtil.dumpToWKTFile(currentPartitions, currentWKTPath);
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

	public static void reorganizePartitions(Path path, ArrayList<Partition> splitPartitions, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		ArrayList<Partition> currentPartitions = MetadataUtil.getPartitions(path, params);

		@SuppressWarnings("deprecation")
		Job job = new Job(params, "RTreeReorganizer");
		job.setJarByClass(RTreeReorganizer.class);
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		double blockSize = Double.parseDouble(conf.get("dfs.blocksize"));
		String sindex = params.get("sindex");

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

		for (Partition partition : splitPartitions) {
			totalSplitSize += partition.size;
			totalSplitBlocks += partition.getNumberOfBlock(blockSize);
			FileUtil.copy(fs, new Path(path, partition.filename), fs, new Path(tempInputPath, partition.filename),
					false, true, conf);
		}

		System.out.println("Total split partitions = " + splitPartitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);

		MetadataUtil.dumpToFile(splitPartitions, path, "rects.split");
		ArrayList<Partition> partitonsToRemove = new ArrayList<Partition>();
		for (Partition splitPartition : splitPartitions) {
			for (Partition currentPartition : currentPartitions) {
				if (currentPartition.cellId == splitPartition.cellId) {
					partitonsToRemove.add(currentPartition);
				}
			}
		}
		currentPartitions.removeAll(partitonsToRemove);
		MetadataUtil.dumpToFile(currentPartitions, path, "rects.keep");

		IncrementalRTreeFilePartitioner partitioner = new IncrementalRTreeFilePartitioner();
		partitioner.createFromInputFile(path, tempOutputPath, params);
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
		ArrayList<Partition> tempPartitions = MetadataUtil.getPartitions(tempOutputPath, params);

		for (Partition p : tempPartitions) {
			fs.rename(new Path(tempOutputPath, p.filename), new Path(path, p.filename));
		}

		currentPartitions.addAll(tempPartitions);

		// Update master and wkt file
		Path currentMasterPath = new Path(path, "_master." + sindex);
		fs.delete(currentMasterPath);
		MetadataUtil.dumpToFile(currentPartitions, currentMasterPath);
		Path currentWKTPath = new Path(path, "_" + sindex + ".wkt");
		MetadataUtil.dumpToWKTFile(currentPartitions, currentWKTPath);

		fs.delete(tempInputPath);
		fs.delete(tempOutputPath);

		for (Partition p : splitPartitions) {
			fs.delete(new Path(path, p.filename));
		}
	}
}
