/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat;

/**
 * @author eldawy
 *
 */
public class HDFInputFormat extends SpatialInputFormat<NASADataset, Iterable<NASAShape>> {

  public HDFInputFormat() {
  }
  
  @Override
  public RecordReader<NASADataset, Iterable<NASAShape>> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new HDFRecordReader(job, (FileSplit) split,
        job.get(HDFRecordReader.DatasetName),
        job.getBoolean(HDFRecordReader.SkipFillValue, true));
  }

}
