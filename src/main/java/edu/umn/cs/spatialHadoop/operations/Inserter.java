package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.IndexOutputFormat;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;
import edu.umn.cs.spatialHadoop.indexing.Indexer.PartitionerMap;
import edu.umn.cs.spatialHadoop.indexing.Indexer.PartitionerReduce;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

public class Inserter {

	private static final Log LOG = LogFactory.getLog(Inserter.class);

	// Insert Mapper
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

	// Insert Reducer
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

	private static void insertLocal(Path currentPath, Path insertPath, OperationsParams params) {

	}

	private static Job insertMapReduce(Path currentPath, Path insertPath, OperationsParams params) {
		Job job = new Job(params, "Inserter");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Indexer.class);

		// Set the correct partitioner according to index type
		String index = conf.get("sindex");
		if (index == null)
			throw new RuntimeException("Index type is not set");
		long t1 = System.currentTimeMillis();
//		setLocalIndexer(conf, index);
		Partitioner partitioner = createPartitioner(currentPath, conf, index);
		Partitioner.setPartitioner(conf, partitioner);

		long t2 = System.currentTimeMillis();
		System.out.println("Total time for space subdivision in millis: " + (t2 - t1));

		// Set mapper and reducer
		Shape shape = OperationsParams.getShape(conf, "shape");
		job.setMapperClass(InserterMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setReducerClass(InserterReduce.class);
		// Set input and output
		job.setInputFormatClass(SpatialInputFormat3.class);
		SpatialInputFormat3.setInputPaths(job, currentPath);
//		job.setOutputFormatClass(IndexOutputFormat.class);
//		IndexOutputFormat.setOutputPath(job, outPath);
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
	
	public static Partitioner createPartitioner(Path in, Path out,
		      Configuration job, String partitionerName) throws IOException {
		    return createPartitioner(new Path[] {in}, out, job, partitionerName);
		  }

	public static Job insert(Path currentPath, Path insertPath, OperationsParams params)
			throws IOException, InterruptedException {
		if (OperationsParams.isLocal(new JobConf(params), currentPath)) {
			insertLocal(currentPath, insertPath, params);
			return null;
		} else {
			return insertMapReduce(currentPath, insertPath, params);
		}
	}

	private static void printUsage() {
		System.out.println("Insert data from a file to another file with same type of shape");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 2)) {
			printUsage();
			System.exit(1);
		}

		Path originalFile = inputFiles[0];
		Path newFile = inputFiles[1];
		System.out.println(originalFile);
		System.out.println(newFile);
	}

}
