/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * An interface for local indexing partitions.
 * @author Ahmed Eldawy
 *
 */
public interface LocalIndexer {
  
  /**The name of the configuration line that stores local indexer class*/
  public static final String LocalIndexerClass = "LocalIndexer.ClassName";
  
  /**
   * Setup this instance of LocalIndexer for the specified job.
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  void setup(Configuration conf) throws IOException, InterruptedException;
  
  
  /**
   * Returns the default extension for this local index. 
   */
  String getExtension();
  
  /**
   * Build a local index for a file that is not yet indexed.
   * @param nonIndexedFile - path to the file that contains input records.
   *   The input file is in the local file system (not in HDFS).
   * @param outputIndexedFile - path to the file that will contain the indexed
   *   file. The output file might be in HDFS.
   * @param shape - The shape that is stored in the input file.
   * @throws IOException
   * @throws InterruptedException
   */
  void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile, Shape shape)
      throws IOException, InterruptedException;
}
