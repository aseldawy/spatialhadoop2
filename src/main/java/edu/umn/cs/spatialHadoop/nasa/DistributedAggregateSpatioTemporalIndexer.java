/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialHadoop.util.NASADatasetUtil;
import edu.umn.cs.spatialHadoop.util.TemporalIndexManager;

/**
 * Distributed version of the spatio-temporal indexer of SHAHED (AggregateQuadTree).
 * Hadoop Map-Reduce framework is used to implement the distribution algorithm.
 * 
 * @author ibrahimsabek
 *
 */
public class DistributedAggregateSpatioTemporalIndexer {

	/** Logger */
	private static final Log LOG = LogFactory
			.getLog(DistributedAggregateSpatioTemporalIndexer.class);

	private static final String HDFSIndexPath = "DistributedAggregateSpatioTemporalIndexer.HDFSIndexPath";

	private static Path hdfsIndexPath = null;

	public static class AggregateQuadTreeMaper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// private Text success = new Text("true");
		// private Text failure = new Text("false");

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			setIndexPath(new Path(job.get(HDFSIndexPath)));
		}

		@Override
		public void map(LongWritable dummy, Text hdfFilePathText,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			if (hdfsIndexPath == null) {
				LOG.warn("Index path for " + hdfFilePathText.toString()
						+ " is not setted");
				// output.collect(hdfFilePathText, failure);
				return;
			}

			// create a template AggregateQuadTree node
			AggregateQuadTree.getOrCreateStockQuadTree(1200);

			String hdfFilePathString = hdfFilePathText.toString();
			Path hdfFilePath = new Path(hdfFilePathString);
			String hdfIndexFileName = hdfFilePathString.substring(
					hdfFilePathString.lastIndexOf("/") + 1,
					hdfFilePathString.length());
			Path hdfIndexFilePath = new Path(hdfsIndexPath.toString() + "/"
					+ hdfIndexFileName);
			try {
				AggregateQuadTree.build(new Configuration(), hdfFilePath,
						"LST_Day_1km", hdfIndexFilePath);
				// output.collect(hdfFilePathText, success);
			} catch (Exception e) {
				throw new RuntimeException("Error in mapper", e);
			}

		}
	}

	private static void printUsage() {
		System.out
				.println("Performs a spatio-temporal indexing for data stored in hadoop");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<dataset path> - (*) Path to input dataset");
		System.out.println("<index path> - (*) Path to index output");
		System.out.println("time:yyyy.mm.dd..yyyy.mm.dd - (*) Time range");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Build a bunch of AggregateQuadTrees using a Map-Reduce job
	 * 
	 * @param inputPathsDictionaryPath
	 * @param params
	 * @throws IOException
	 */
	public static void aggregateQuadTreeMapReduce(
			Path inputPathsDictionaryPath, OperationsParams params)
			throws IOException {

		// configure a map-reduce job
		JobConf job = new JobConf(params,
				DistributedAggregateSpatioTemporalIndexer.class);

		Path outputPath;
		String outputPathPrefix = "aggQuadTree_";
		FileSystem outFs = FileSystem.get(job);
		do {
			outputPath = new Path(outputPathPrefix
					+ (int) (Math.random() * 1000000));
		} while (outFs.exists(outputPath));

		job.setJobName("AggregateQuadTree");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(AggregateQuadTreeMaper.class);
		job.set(HDFSIndexPath, hdfsIndexPath.toString());

		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, inputPathsDictionaryPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		if (job.getBoolean("local", false)) {
			// Enforce local execution if explicitly set by user or for small
			// files
			job.set("mapred.job.tracker", "local");
			// Use multithreading too
			job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, 16);
		}
		job.setNumReduceTasks(0);

		// Submit the job
		JobClient.runJob(job);

		outFs.delete(outputPath, true);
	}

	/**
	 * Set the HDFS path of the desired index
	 * 
	 * @param hdfsIndexPath
	 */
	public static void setIndexPath(Path hdfsIndexPath) {
		DistributedAggregateSpatioTemporalIndexer.hdfsIndexPath = hdfsIndexPath;
	}

	public static void main(String[] args) throws IOException, ParseException {

		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args), false);

		final Path[] paths = params.getPaths();
		if (paths.length <= 1 && !params.checkInput()) {
			printUsage();
			System.exit(1);
		}
		if (paths.length >= 2 && paths[1] == null) {
			printUsage();
			System.exit(1);
		}
		if (params.get("time") == null) {
			System.err.println("You must provide a time range");
			printUsage();
			System.exit(1);
		}

		Path datasetPath = paths[0]; // dataset path
		Path indexesPath = paths[1]; // index path
		String timeRange = params.get("time"); // time range

		TemporalIndexManager temporalIndexManager = new TemporalIndexManager(
				datasetPath, indexesPath);
		temporalIndexManager.prepareNeededIndexes(timeRange);

		// Indexes need to be built or re-built using AggregateQuadTreeMapReduce
		Path[] dailyIndexes = temporalIndexManager.getNeededDailyIndexes();
		LOG.info("Needs to index/re-index " + dailyIndexes.length + " days");
		for (Path dailyIndexPath : dailyIndexes) {
			FileSystem currFileSystem = dailyIndexPath.getFileSystem(params);
			Path[] dailyIndexHDFFiles = FileUtil
					.getFilesListInPath(dailyIndexPath);
			Path dailyIndexDictionaryPath = FileUtil.writePathsToHDFSFile(
					params, dailyIndexHDFFiles);
			Path dailyIndexOutputPath = new Path(temporalIndexManager
					.getDailyIndexesHomePath().toString()
					+ "/"
					+ NASADatasetUtil.extractDateStringFromPath(dailyIndexPath));

			if (currFileSystem.exists(dailyIndexOutputPath)) {
				currFileSystem.delete(dailyIndexOutputPath, true);
			}
			currFileSystem.mkdirs(dailyIndexOutputPath);

			DistributedAggregateSpatioTemporalIndexer.setIndexPath(dailyIndexOutputPath);
			aggregateQuadTreeMapReduce(dailyIndexDictionaryPath, params);

			currFileSystem.delete(dailyIndexDictionaryPath, false);
		}

		// Indexes need to be merged or re-merged
		Path[] monthlyIndexes = temporalIndexManager.getNeededMonthlyIndexes();
		LOG.info("Needs to index/re-index " + monthlyIndexes.length + " months");
		for (Path monthlyIndexPath : monthlyIndexes) {
			FileSystem currFileSystem = monthlyIndexPath
					.getFileSystem(new Configuration());
			ArrayList<Path[]> pathsArrList = NASADatasetUtil
					.getSortedTuplesInPath(temporalIndexManager
							.getDailyIndexesHomePath(), NASADatasetUtil
							.extractDateStringFromPath(monthlyIndexPath));

			if (currFileSystem.exists(monthlyIndexPath)) {
				currFileSystem.delete(monthlyIndexPath, true);
			}
			currFileSystem.mkdirs(monthlyIndexPath);

			for (Path[] currDailyIndexHDFFiles : pathsArrList) {
				Path currMonthlyIndexHDFFilePath = new Path(
						monthlyIndexPath.toString()
								+ "/"
								+ NASADatasetUtil
										.getHDFfilePattern(currDailyIndexHDFFiles[0]
												.toString()) + ".hdf");
				AggregateQuadTree.merge(new Configuration(),
						currDailyIndexHDFFiles, currMonthlyIndexHDFFilePath);
			}
		}

		// Indexes need to be merged or re-merged
		Path[] yearlyIndexes = temporalIndexManager.getNeededYearlyIndexes();
		LOG.info("Needs to index/re-index " + yearlyIndexes.length + " years");
		for (Path yearlyIndexPath : yearlyIndexes) {
			FileSystem currFileSystem = yearlyIndexPath
					.getFileSystem(new Configuration());
			ArrayList<Path[]> pathsArrList = NASADatasetUtil
					.getSortedTuplesInPath(temporalIndexManager
							.getMonthlyIndexesHomePath(), NASADatasetUtil
							.extractDateStringFromPath(yearlyIndexPath));

			if (currFileSystem.exists(yearlyIndexPath)) {
				currFileSystem.delete(yearlyIndexPath, true);
			}
			currFileSystem.mkdirs(yearlyIndexPath);

			for (Path[] currMonthlyIndexHDFFiles : pathsArrList) {
				Path currYearlyIndexHDFFilePath = new Path(
						yearlyIndexPath.toString()
								+ "/"
								+ NASADatasetUtil
										.getHDFfilePattern(currMonthlyIndexHDFFiles[0]
												.toString()) + ".hdf");
				AggregateQuadTree.merge(new Configuration(),
						currMonthlyIndexHDFFiles, currYearlyIndexHDFFilePath);
			}
		}

	}

}
