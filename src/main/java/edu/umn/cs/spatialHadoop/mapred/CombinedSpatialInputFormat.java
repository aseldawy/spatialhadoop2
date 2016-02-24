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
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

/**
 * An input format that combines a set of files and returns one bunch of
 * FileSplits used as input for the Map-Reduce jobs
 * 
 * @author Ibrahim Sabek
 */

public class CombinedSpatialInputFormat<S extends Shape> extends
		SpatialInputFormat<Rectangle, S> {

	private static final Log LOG = LogFactory
			.getLog(CombinedSpatialInputFormat.class);

	private static final double SPLIT_SLOP = 1.1; // 10% slop

	@Override
	public InputSplit[] getSplits(final JobConf job, int numSplits)
			throws IOException {

		final Path[] inputFiles = getInputPaths(job);
		final Vector<InputSplit> combinedSplits = new Vector<InputSplit>();
		InputSplit[][] inputSplits = new InputSplit[inputFiles.length][];

		@SuppressWarnings("unchecked")
		GlobalIndex<Partition> gIndexes[] = new GlobalIndex[inputFiles.length];
		for (int i_file = 0; i_file < inputFiles.length; i_file++) {
			FileSystem fs = inputFiles[i_file].getFileSystem(job);
			gIndexes[i_file] = SpatialSite.getGlobalIndex(fs,
					inputFiles[i_file]);
			if (gIndexes[i_file] != null) {
				final Path currentInputFile = inputFiles[i_file];
				CellInfo[] cellsInfo = SpatialSite.cellsOf(fs,
						inputFiles[i_file]);
				for (CellInfo cellInfo : cellsInfo) {
					gIndexes[i_file].rangeQuery(cellInfo,
							new ResultCollector<Partition>() {
								@Override
								public void collect(Partition p) {
									try {
										List<FileSplit> fileSplits = new ArrayList<FileSplit>();
										Path splitPath = new Path(
												currentInputFile, p.filename);
										splitFile(job, splitPath, fileSplits);

										for (FileSplit fileSplit : fileSplits) {
											combinedSplits.add(fileSplit);
										}
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
							});
				}
			} else {
				JobConf temp = new JobConf(job);
				setInputPaths(temp, inputFiles[i_file]);
				inputSplits[i_file] = super.getSplits(temp, 1);
				for (InputSplit currentSplit : inputSplits[i_file]) {
					combinedSplits.add(currentSplit);
				}
			}

		}
		LOG.info("Combined " + combinedSplits.size() + " file splits");

		return combinedSplits.toArray(new InputSplit[combinedSplits.size()]);
	}

	public void splitFile(JobConf job, Path path, List<FileSplit> splits)
			throws IOException {
		NetworkTopology clusterMap = new NetworkTopology();
		FileSystem fs = path.getFileSystem(job);
		FileStatus file = fs.getFileStatus(path);
		long length = file.getLen();
		BlockLocation[] blkLocations = fs
				.getFileBlockLocations(file, 0, length);
		if (length != 0) {
			long blockSize = file.getBlockSize();
			long splitSize = blockSize;

			long bytesRemaining = length;
			while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
				String[] splitHosts = getSplitHosts(blkLocations, length
						- bytesRemaining, splitSize, clusterMap);
				splits.add(new FileSplit(path, length - bytesRemaining,
						splitSize, splitHosts));
				bytesRemaining -= splitSize;
			}

			if (bytesRemaining != 0) {
				splits.add(new FileSplit(path, length - bytesRemaining,
						bytesRemaining, blkLocations[blkLocations.length - 1]
								.getHosts()));
			}
		} else if (length != 0) {
			String[] splitHosts = getSplitHosts(blkLocations, 0, length,
					clusterMap);
			splits.add(new FileSplit(path, 0, length, splitHosts));
		} else {
			// Create empty hosts array for zero length files
			splits.add(new FileSplit(path, 0, length, new String[0]));
		}
	}

	@Override
	public RecordReader<Rectangle, S> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		if (reporter != null)
			reporter.setStatus(split.toString());
		this.rrClass = ShapeRecordReader.class;
		return super.getRecordReader(split, job, reporter);
	}

}
