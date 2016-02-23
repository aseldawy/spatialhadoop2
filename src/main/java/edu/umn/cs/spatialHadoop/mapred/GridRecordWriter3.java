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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A record writer to be used to write the output of MapReduce programs to
 * a spatial index. The key is a rectangle which indicates the MBR of the
 * partition and value is a shape. This record writer does not replicate the
 * given shape to partition (i.e., write it only to the given partition). If
 * the provided rectangle (key) does not match any of the existing partitions,
 * a new partition is created with the given boundaries.
 * @author Ahmed Eldawy
 *
 * @param <S>
 */
public class GridRecordWriter3<S extends Shape>
extends edu.umn.cs.spatialHadoop.core.GridRecordWriter<S> implements RecordWriter<Rectangle, S> {

  public GridRecordWriter3(JobConf job, String name, CellInfo[] cells) throws IOException {
    super(null, job, name, cells);
  }
  
  @Override
  public void write(Rectangle key, S value) throws IOException {
    
    super.write(key, value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
