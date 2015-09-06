/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import org.apache.hadoop.conf.Configuration;

import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

/**
 * An interface for filtering blocks before running map tasks.
 * @author Ahmed Eldawy
 *
 */
public interface BlockFilter {

  /**
   * Configure the block filter the first time it is created.
   * @param conf
   */
  public void configure(Configuration conf);

  /**
   * Selects the blocks that need to be processed b a MapReduce job.
   * @param gIndex
   * @param output
   */
  public void selectCells(GlobalIndex<Partition> gIndex,
      ResultCollector<Partition> output);
  
  /**
   * Selects block pairs that need to be processed together by a binary
   * MapReduce job. A binary MapReduce job is a job that deals with two input
   * files that need to be processed together (e.g., spatial join).
   * @param gIndex1
   * @param gIndex2
   */
  public void selectCellPairs(GlobalIndex<Partition> gIndex1,
      GlobalIndex<Partition> gIndex2,
      ResultCollector2<Partition, Partition> output);
}
