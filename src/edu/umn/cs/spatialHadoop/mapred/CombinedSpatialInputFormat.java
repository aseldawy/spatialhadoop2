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
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

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

	@Override
	public InputSplit[] getSplits(final JobConf job, int numSplits)
			throws IOException {

		final Path[] inputFiles = getInputPaths(job);
		final Vector<InputSplit> combinedSplits = new Vector<InputSplit>();
		InputSplit[][] inputSplits = new InputSplit[inputFiles.length][];
		for (int i_file = 0; i_file < inputFiles.length; i_file++) {
			JobConf temp = new JobConf(job);
			setInputPaths(temp, inputFiles[i_file]);
			inputSplits[i_file] = super.getSplits(temp, 1);
			for (InputSplit currentSplit : inputSplits[i_file]) {
				combinedSplits.add(currentSplit);
			}
		}
		LOG.info("Combined " + combinedSplits.size() + " file splits");

		return combinedSplits.toArray(new InputSplit[combinedSplits.size()]);
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
