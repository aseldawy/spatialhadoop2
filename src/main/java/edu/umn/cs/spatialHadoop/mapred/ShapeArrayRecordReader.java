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
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;


/**
 * Reads a file as a list of RTrees
 * @author Ahmed Eldawy
 *
 */
public class ShapeArrayRecordReader extends SpatialRecordReader<Rectangle, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ShapeArrayRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private Shape shape;
  
  public ShapeArrayRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    shape = OperationsParams.getShape(conf, "shape");
  }
  
  public ShapeArrayRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    shape = OperationsParams.getShape(job, "shape");
  }

  public ShapeArrayRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, ArrayWritable shapes) throws IOException {
    // Get cellInfo for the current position in file
    boolean element_read = nextShapes(shapes);
    key.set(cellMbr); // Set the cellInfo for the last block read
    return element_read;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(shape.getClass());
  }
  
}
