/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
 * Distributed version of Spatio-Temporal Indexer based on AggregateQuadTree.
 * Hadoop Map-Reduce framework is used to implement the distribution algorithm.
 * 
 * @author ibrahimsabek
 *
 */
public class DistributedSpatioTemporalIndexer {

	/** Logger */
	private static final Log LOG = LogFactory
			.getLog(DistributedSpatioTemporalIndexer.class);

  private static final String HDFSIndexPath =
      "DistributedSpatioTemporalIndexer.HDFSIndexPath";

	private static Path hdfsIndexPath = null;

	public static class AggregateQuadTreeMaper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text success = new Text("true");
		private Text failure = new Text("false");
		
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
				output.collect(hdfFilePathText, failure);
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
				output.collect(hdfFilePathText, success);
			} catch (Exception e) {
			  throw new RuntimeException("Error in mapper", e);
			}

		}
	}

	/**
	 * Build a bunch of AggregateQuadTrees using a Map-Reduce job
	 * 
	 * @param inputPathsDictionaryPath
	 * @param params
	 * @return
	 * @throws IOException
	 */
	public static void aggregateQuadTreeMapReduce(
			Path inputPathsDictionaryPath, OperationsParams params)
			throws IOException {

		// configure a map-reduce job
		JobConf job = new JobConf(params,
				DistributedSpatioTemporalIndexer.class);

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
      // Enforce local execution if explicitly set by user or for small files
      job.set("mapred.job.tracker", "local");
      // Use multithreading too
      job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    }
    job.setNumReduceTasks(0);

		// Submit the job
		JobClient.runJob(job);
		
		outFs.deleteOnExit(outputPath);
	}

	/**
	 * Set the HDFS path of the desired index
	 * 
	 * @param hdfsIndexPath
	 */
	public static void setIndexPath(Path hdfsIndexPath) {
		DistributedSpatioTemporalIndexer.hdfsIndexPath = hdfsIndexPath;
	}

	public static void main(String[] args) throws IOException, ParseException {

		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args), false);
		String timeRange = params.get("time"); //time range
		Path datasetPath = params.getPaths()[0]; //dataset path
		Path indexesPath = params.getPaths()[1]; //index path

		TemporalIndexManager temporalIndexManager = new TemporalIndexManager(
				datasetPath, indexesPath);
		temporalIndexManager.prepareNeededIndexes(timeRange);

		// Indexes need to be built or re-built using AggregateQuadTreeMapReduce
		Path[] dailyIndexes = temporalIndexManager.getNeededDailyIndexes();
		for (Path dailyIndexPath : dailyIndexes) {	
			FileSystem currFileSystem = dailyIndexPath.getFileSystem(new Configuration());
			Path[] dailyIndexHDFFiles = NASADatasetUtil.getFilesListInPath(dailyIndexPath);
			Path dailyIndexDictionaryPath = FileUtil.writePathsToFile(params,
					dailyIndexHDFFiles);
			Path dailyIndexOutputPath = new Path(temporalIndexManager
					.getDailyIndexesHomePath().toString() + "/"
					+ NASADatasetUtil.extractDateStringFromPath(dailyIndexPath));
			
			if(currFileSystem.exists(dailyIndexOutputPath)){
				currFileSystem.delete(dailyIndexOutputPath, true);
			}
			currFileSystem.mkdirs(dailyIndexOutputPath);
			
			DistributedSpatioTemporalIndexer.setIndexPath(dailyIndexOutputPath);
			aggregateQuadTreeMapReduce(dailyIndexDictionaryPath, params);

			currFileSystem.delete(dailyIndexDictionaryPath, false);
		}

		// Indexes need to be merged or re-merged
		Path[] monthlyIndexes = temporalIndexManager.getNeededMontlyIndexes();
		for (Path monthlyIndexPath : monthlyIndexes) {
			FileSystem currFileSystem = monthlyIndexPath.getFileSystem(new Configuration());
			ArrayList<Path[]> pathsArrList = NASADatasetUtil.getSortedTuplesInPath(temporalIndexManager.getDailyIndexesHomePath(),
					NASADatasetUtil.extractDateStringFromPath(monthlyIndexPath));
			
			if(currFileSystem.exists(monthlyIndexPath)){
				currFileSystem.delete(monthlyIndexPath, true);
			}
			currFileSystem.mkdirs(monthlyIndexPath);
			
			for(Path[] currDailyIndexHDFFiles: pathsArrList){
				Path currMonthlyIndexHDFFilePath = new Path(monthlyIndexPath.toString() + 
						"/" + NASADatasetUtil.getHDFfilePattern(currDailyIndexHDFFiles[0].toString()) +".hdf");
				AggregateQuadTree.merge(new Configuration(), currDailyIndexHDFFiles, currMonthlyIndexHDFFilePath);
			}			
		}

		// Indexes need to be merged or re-merged
		Path[] yearlyIndexes = temporalIndexManager.getNeededYearlyIndexes();
		for (Path yearlyIndexPath : yearlyIndexes) {
			FileSystem currFileSystem = yearlyIndexPath.getFileSystem(new Configuration());
			ArrayList<Path[]> pathsArrList = NASADatasetUtil.getSortedTuplesInPath(temporalIndexManager.getMonthlyIndexesHomePath(),
					NASADatasetUtil.extractDateStringFromPath(yearlyIndexPath));
			
			if(currFileSystem.exists(yearlyIndexPath)){
				currFileSystem.delete(yearlyIndexPath, true);
			}
			currFileSystem.mkdirs(yearlyIndexPath);
			
			for(Path[] currMonthlyIndexHDFFiles: pathsArrList){
				Path currYearlyIndexHDFFilePath = new Path(yearlyIndexPath.toString() + 
						"/" + NASADatasetUtil.getHDFfilePattern(currMonthlyIndexHDFFiles[0].toString()) +".hdf");
				AggregateQuadTree.merge(new Configuration(), currMonthlyIndexHDFFiles, currYearlyIndexHDFFilePath);
			}			
		}

	}

}
