/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * Indexes local files using R-tree.
 * @author Ahmed Eldawy
 *
 */
public class RTreeLocalIndexer implements LocalIndexer {
  private static final Log LOG = LogFactory.getLog(RTreeLocalIndexer.class);

  /**Configuration of the running job*/
  protected Configuration conf;

  @Override
  public void setup(Configuration conf) throws IOException,
      InterruptedException {
    this.conf = conf;
  }

  @Override
  public String getExtension() {
    return "rtree";
  }
  
  @Override
  public void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile,
      Shape shape) throws IOException, InterruptedException {
    // Read all data of the written file in memory
    byte[] cellData = new byte[(int) nonIndexedFile.length()];
    InputStream cellIn = new BufferedInputStream(new FileInputStream(nonIndexedFile));
    cellIn.read(cellData);
    cellIn.close();

    // Build an RTree over the elements read from file
    // Create the output file
    FileSystem outFS = outputIndexedFile.getFileSystem(conf);
    DataOutputStream cellStream = outFS.create(outputIndexedFile);
    cellStream.writeLong(SpatialSite.RTreeFileMarker);
    int degree = 4096 / RTree.NodeSize;
    boolean fastAlgorithm = conf.get(SpatialSite.RTREE_BUILD_MODE, "fast").equals("fast");
    RTree.bulkLoadWrite(cellData, 0, cellData.length, degree, cellStream,
        shape.clone(), fastAlgorithm);
    cellStream.close();
  }

}
