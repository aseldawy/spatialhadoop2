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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A record reader for objects of class {@link Shape}
 * @author Ahmed Eldawy
 *
 */
public class ShapeLineRecordReader
    extends SpatialRecordReader<Rectangle, Text> {

  public ShapeLineRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
  }

  public ShapeLineRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
  }
  
  public ShapeLineRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    super(in, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, Text shapeLine) throws IOException {
    boolean read_line = nextLine(shapeLine);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public Text createValue() {
    return new Text();
  }
}
