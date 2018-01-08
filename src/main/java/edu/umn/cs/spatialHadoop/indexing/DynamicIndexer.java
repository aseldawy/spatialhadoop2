package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.IncrementalRTreeIndexer.InserterMap;
import edu.umn.cs.spatialHadoop.indexing.IncrementalRTreeIndexer.InserterReduce;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class DynamicIndexer {

	public static class Appender {
		private static Job insertMapReduce(Path currentPath, Path appendPath, OperationsParams params)
				throws IOException, InterruptedException, ClassNotFoundException, InstantiationException,
				IllegalAccessException {
			@SuppressWarnings("deprecation")
			Job job = new Job(params, "Inserter");
			Configuration conf = job.getConfiguration();
			job.setJarByClass(Inserter.class);

			// Set input file MBR if is is not already set
			Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
			if (inputMBR == null) {
				inputMBR = FileMBR.fileMBR(currentPath, new OperationsParams(conf));
				OperationsParams.setShape(conf, "mbr", inputMBR);
			}

			String index = conf.get("sindex");
			if (index == null)
				throw new RuntimeException("Index type is not set");
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
			job.setNumReduceTasks(Math.max(1,
					Math.min(partitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));

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
	}

	public static void main(String[] args) {
		System.out.println("Dynamic indexer - test");
	}
}
