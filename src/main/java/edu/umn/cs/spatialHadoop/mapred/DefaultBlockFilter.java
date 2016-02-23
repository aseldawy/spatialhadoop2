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
 * A default implementation for BlockFilter that returns everything.
 * @author eldawy
 *
 */
public class DefaultBlockFilter implements BlockFilter {
  
  @Override
  public void configure(Configuration conf) {
    // Do nothing
  }

  @Override
  public void selectCells(GlobalIndex<Partition> gIndex,
      ResultCollector<Partition> output) {
    // Do nothing
  }

  @Override
  public void selectCellPairs(GlobalIndex<Partition> gIndex1,
      GlobalIndex<Partition> gIndex2,
      ResultCollector2<Partition, Partition> output) {
    // Do nothing
  }

}
