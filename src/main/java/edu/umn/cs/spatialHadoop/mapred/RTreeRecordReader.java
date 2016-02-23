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

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.RTree;


/**
 * Reads a file as a list of RTrees
 * @author Ahmed Eldawy
 *
 */
public class RTreeRecordReader<S extends Shape> extends SpatialRecordReader<Rectangle, RTree<S>> {
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private S stockShape;
  
  public RTreeRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    stockShape = (S) SpatialSite.createStockShape(conf);
  }
  
  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    stockShape = (S) SpatialSite.createStockShape(job);
  }

  public RTreeRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, RTree<S> rtree) throws IOException {
    boolean read_line = nextRTree(rtree);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public RTree<S> createValue() {
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockShape);
    return rtree;
  }
}
