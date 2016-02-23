/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;

public class RTreeGridRecordWriter<S extends Shape>
    extends edu.umn.cs.spatialHadoop.core.RTreeGridRecordWriter<S>
    implements RecordWriter<IntWritable, S> {

  public RTreeGridRecordWriter(JobConf job, String prefix, CellInfo[] cells) throws IOException {
    super(null, job, prefix, cells);
  }
  
  @Override
  public void write(IntWritable key, S value) throws IOException {
    super.write(key.get(), value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
