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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.NASADataset;
import edu.umn.cs.spatialHadoop.core.NASAPoint;
import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * @author Ahmed Eldawy
 *
 */
public class HDFInputFormat extends FileInputFormat<NASADataset, NASAPoint> {
  
  /**The configuration entry for skipping the fill value*/
  public static final String SkipFillValue = "HDFRecordReader.SkipFillValue";
  
  /**Configuration for name of the dataset to read from HDF file*/
  public static final String DatasetName = "HDFInputFormat.DatasetName";

  @Override
  public RecordReader<NASADataset, NASAPoint> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new HDFRecordReader(job, (FileSplit)split, job.get(DatasetName), job.getBoolean(SkipFillValue, true));
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // HDF files cannot be split
    return false;
  }
}
