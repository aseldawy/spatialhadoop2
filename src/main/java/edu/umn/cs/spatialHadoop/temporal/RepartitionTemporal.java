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

package edu.umn.cs.spatialHadoop.temporal;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.CombinedSpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RTreeGridOutputFormat;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.util.NASADatasetUtil;
import edu.umn.cs.spatialHadoop.util.TemporalIndexManager;

/**
 * Supports indexing for spatial shapes on higher temporal levels (e.g. weeks,
 * months and years) by aggregating indexes on lower levels in one higher level.
 * 
 * @author ibrahimsabek
 */
public class RepartitionTemporal extends Repartition {

	static final Log LOG = LogFactory.getLog(RepartitionTemporal.class);

	public RepartitionTemporal() {
		super();
	}

	public static void repartitionMapReduce(Path[] inputPaths, Path outputPath,
			OperationsParams params) throws IOException, InterruptedException {
		String sindex = params.get("sindex");
		boolean overwrite = params.getBoolean("overwrite", false);
		Shape stockShape = params.getShape("shape");

		FileSystem outFs = outputPath.getFileSystem(params);

		@SuppressWarnings("deprecation")
		final long blockSize = outFs.getDefaultBlockSize();

		// Calculate the dimensions of each partition based on gindex type
		CellInfo[] cellInfos;
		if (sindex.equals("grid")) {
			Rectangle inputMBR = FileMBR.fileMBR(inputPaths[0], params);
			long inputFileSize = FileMBR.sizeOfLastProcessedFile;
			for (int i = 1; i < inputPaths.length; i++) {
				Rectangle currentInputMBR = FileMBR.fileMBR(inputPaths[i],
						params);
				inputMBR.expand(currentInputMBR);
				inputFileSize = inputFileSize + FileMBR.sizeOfLastProcessedFile;
			}

			int num_partitions = calculateNumberOfPartitions(
					new Configuration(), inputFileSize, outFs, outputPath,
					blockSize);

			GridInfo gridInfo = new GridInfo(inputMBR.x1, inputMBR.y1,
					inputMBR.x2, inputMBR.y2);
			gridInfo.calculateCellDimensions(num_partitions);
			cellInfos = gridInfo.getAllCells();
		} else if (sindex.equals("rtree") || sindex.equals("r+tree")
				|| sindex.equals("str") || sindex.equals("str+")) {
			// Pack in rectangles using an RTree
			cellInfos = packInRectangles(inputPaths, outputPath, params, null);
		} else {
			throw new RuntimeException("Unsupported spatial index: " + sindex);
		}

		JobConf job = new JobConf(params, RepartitionTemporal.class);
		job.setJobName("RepartitionTemporal");

		// Overwrite output file
		if (outFs.exists(outputPath)) {
			if (overwrite)
				outFs.delete(outputPath, true);
			else
				throw new RuntimeException("Output file '" + outputPath
						+ "' already exists and overwrite flag is not set");
		}

		// Decide which map function to use depending on the type of global
		// index
		if (sindex.equals("rtree") || sindex.equals("str")) {
			// Repartition without replication
			job.setMapperClass(RepartitionMapNoReplication.class);
		} else {
			// Repartition with replication (grid, str+, and r+tree)
			job.setMapperClass(RepartitionMap.class);
		}
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(stockShape.getClass());
		CombinedSpatialInputFormat.setInputPaths(job, inputPaths);
		job.setInputFormat(CombinedSpatialInputFormat.class);

		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

		FileOutputFormat.setOutputPath(job, outputPath);
		if (sindex.equals("grid") || sindex.equals("str")
				|| sindex.equals("str+")) {
			job.setOutputFormat(GridOutputFormat.class);
		} else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
			// For now, the two types of local index are the same
			job.setOutputFormat(RTreeGridOutputFormat.class);
		} else {
			throw new RuntimeException("Unsupported spatial index: " + sindex);
		}

		SpatialSite.setCells(job, cellInfos);
		job.setBoolean(SpatialSite.OVERWRITE, overwrite);

		// Set reduce function
		job.setReducerClass(RepartitionReduce.class);
		job.setNumReduceTasks(Math.max(1, Math.min(cellInfos.length,
				(clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

		// Set output committer that combines output files together
		job.setOutputCommitter(RepartitionOutputCommitter.class);

		JobClient.runJob(job);

	}

	public static void repartitionMapReduce(Path[] inputPaths, Path outputPath,
			Shape stockShape, long blockSize, CellInfo[] cellInfos,
			String sindex, boolean overwrite) throws IOException {

		JobConf job = new JobConf(Repartition.class);

		job.setJobName("RepartitionTemporal");
		FileSystem outFs = outputPath.getFileSystem(job);

		// Overwrite output file
		if (outFs.exists(outputPath)) {
			if (overwrite)
				outFs.delete(outputPath, true);
			else
				throw new RuntimeException("Output file '" + outputPath
						+ "' already exists and overwrite flag is not set");
		}

		// Decide which map function to use depending on the type of global
		// index
		if (sindex.equals("rtree") || sindex.equals("str")) {
			// Repartition without replication
			job.setMapperClass(RepartitionMapNoReplication.class);
		} else {
			// Repartition with replication (grid and r+tree)
			job.setMapperClass(RepartitionMap.class);
		}
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(stockShape.getClass());
		CombinedSpatialInputFormat.setInputPaths(job, inputPaths);
		job.setInputFormat(CombinedSpatialInputFormat.class);

		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

		FileOutputFormat.setOutputPath(job, outputPath);
		if (sindex.equals("grid") || sindex.equals("str")
				|| sindex.equals("str+")) {
			job.setOutputFormat(GridOutputFormat.class);
		} else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
			// For now, the two types of local index are the same
			job.setOutputFormat(RTreeGridOutputFormat.class);
		} else {
			throw new RuntimeException("Unsupported spatial index: " + sindex);
		}

		SpatialSite.setCells(job, cellInfos);
		job.setBoolean(SpatialSite.OVERWRITE, overwrite);

		// Set reduce function
		job.setReducerClass(RepartitionReduce.class);
		job.setNumReduceTasks(Math.max(1, Math.min(cellInfos.length,
				(clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

		// Set output committer that combines output files together
		job.setOutputCommitter(RepartitionOutputCommitter.class);

		if (blockSize != 0) {
			job.setLong("dfs.block.size", blockSize);
			job.setLong("fs.local.block.size", blockSize);
		}

		JobClient.runJob(job);
	}

	private static Path generateIndexPath(Path inputPath, Path outputPath) {
		String inputPathStr = inputPath.toString();
		String inputPathFileName = inputPathStr.substring(
				inputPathStr.lastIndexOf("/") + 1, inputPathStr.length());
		Path indexPath = new Path(outputPath.toString() + "/"
				+ inputPathFileName);
		return indexPath;
	}

	private static void bulkLoadSpatioTemporalIndexesLevel(
			Path indexLevelHomePath, Path[] inputPathDirs, String indexLevel,
			OperationsParams params) throws IOException, InterruptedException {
		LOG.info("Needs to index/re-index " + inputPathDirs.length + " "
				+ indexLevel);

		for (Path inputPathDir : inputPathDirs) {
			FileSystem currFileSystem = inputPathDir.getFileSystem(params);

			if (currFileSystem.exists(inputPathDir)) {
				currFileSystem.delete(inputPathDir, true);
			}
			currFileSystem.mkdirs(inputPathDir);

			ArrayList<Path[]> pathsArrList = NASADatasetUtil
					.getSortedTuplesInPath(indexLevelHomePath, NASADatasetUtil
							.extractDateStringFromPath(inputPathDir));

			Path indexPath = generateIndexPath(pathsArrList.get(0)[0],
					inputPathDir);

			for (Path[] currInputFiles : pathsArrList) {
				repartitionMapReduce(currInputFiles, indexPath, params);
			}

		}

	}

	private static void bulkLoadSpatioTemporalIndexes(
			TemporalIndexManager temporalIndexManager, OperationsParams params)
			throws IOException, InterruptedException {

		bulkLoadSpatioTemporalIndexesLevel(params.getPaths()[0],
				temporalIndexManager.getNeededDailyIndexes(), "daily", params);
		bulkLoadSpatioTemporalIndexesLevel(
				temporalIndexManager.getDailyIndexesHomePath(),
				temporalIndexManager.getNeededMonthlyIndexes(), "monthly",
				params);
		bulkLoadSpatioTemporalIndexesLevel(
				temporalIndexManager.getMonthlyIndexesHomePath(),
				temporalIndexManager.getNeededYearlyIndexes(), "yearly", params);
	}

	protected static void printUsage() {
		System.out.println("Builds a spatio-temporal index for input file");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<dataset path> - (*) Path to input dataset");
		System.out.println("<index path> - (*) Path to index output");
		System.out.println("time:yyyy.mm.dd..yyyy.mm.dd - (*) Time range");
		System.out
				.println("shape:<point|rectangle|polygon> - (*) Type of stored shapes");
		System.out
				.println("sindex:<index> - (*) Type of spatial index (grid|rtree|r+tree|str|str+)");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
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
		if (params.get("sindex") == null) {
			System.err
					.println("Please specify type of index to build (grid, rtree, r+tree, str, str+)");
			printUsage();
			return;
		}
		Path datasetPath = paths[0]; // dataset path
		Path indexesPath = paths[1]; // index path
		String timeRange = params.get("time"); // time range

		TemporalIndexManager temporalIndexManager = new TemporalIndexManager(
				datasetPath, indexesPath);
		temporalIndexManager.prepareNeededIndexes(timeRange);

		// The spatio-temporal index to use
		long t1 = System.currentTimeMillis();
		bulkLoadSpatioTemporalIndexes(temporalIndexManager, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis " + (t2 - t1));

	}

}
