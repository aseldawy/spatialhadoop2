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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A record writer that can be used in MapReduce programs. It writes pairs
 * where the key is {@link NullWritable}(i.e., not provided) and the value
 * is a shape. The given shape is replicated to every cell it overlaps with.
 * @author Ahmed Eldawy
 *
 * @param <S>
 */
public class GridRecordWriter2<S extends Shape>
extends edu.umn.cs.spatialHadoop.core.GridRecordWriter<S> implements RecordWriter<NullWritable, S> {

  public GridRecordWriter2(JobConf job, String name, CellInfo[] cells) throws IOException {
    super(null, job, name, cells);
  }
  
  @Override
  public void write(NullWritable key, S value) throws IOException {
    super.write(key, value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
