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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;


/**
 * Reads a file as a list of RTrees
 * @author Ahmed Eldawy
 *
 */
public class ShapeIterRecordReader extends SpatialRecordReader<Rectangle, ShapeIterator> {
  public static final Log LOG = LogFactory.getLog(ShapeIterRecordReader.class);
  private Shape shape;
  
  public ShapeIterRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    this.shape = OperationsParams.getShape(conf, "shape");
  }
  
  public ShapeIterRecordReader(Configuration conf, FileSplit split)
      throws IOException {
    super(conf, split);
    this.shape = OperationsParams.getShape(conf, "shape");
  }

  public ShapeIterRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }
  
  public void setShape(Shape shape) {
    this.shape = shape;
  }

  @Override
  public boolean next(Rectangle key, ShapeIterator shapeIter) throws IOException {
    // Get cellInfo for the current position in file
    boolean element_read = nextShapeIter(shapeIter);
    key.set(cellMbr); // Set the cellInfo for the last block read
    return element_read;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public ShapeIterator createValue() {
    ShapeIterator shapeIter = new ShapeIterator();
    shapeIter.setShape(shape);
    return shapeIter;
  }
  
}
