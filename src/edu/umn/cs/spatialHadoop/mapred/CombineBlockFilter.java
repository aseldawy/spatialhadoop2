/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.util.Vector;

import org.apache.hadoop.conf.Configuration;

import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

/**
 * A block filter that combines multiple block filters with an AND clause.
 * @author Ahmed Eldawy
 *
 */
public class CombineBlockFilter extends DefaultBlockFilter {

  /**A list of all underlying block filters*/
  private BlockFilter[] blockFilters;

  /**
   * 
   */
  public CombineBlockFilter(BlockFilter bf1, BlockFilter bf2) {
    this.blockFilters = new BlockFilter[] {bf1, bf2};
  }
  
  @Override
  public void configure(Configuration conf) {
    for (BlockFilter bf : blockFilters)
      bf.configure(conf);
  }

  @Override
  public void selectCells(GlobalIndex<Partition> gIndex,
      ResultCollector<Partition> output) {
    final Vector<Partition> selectedSoFar = new Vector<Partition>();
    // First block filter is applied directly to the global index
    blockFilters[0].selectCells(gIndex, new ResultCollector<Partition>() {
      @Override
      public void collect(Partition p) {
        selectedSoFar.add(p);
      }
    });
    // All remaining are served from the partitions selectedSoFar
    for (int i = 1; !selectedSoFar.isEmpty() && i < blockFilters.length; i++) {
      BlockFilter bf = blockFilters[i];
      gIndex = new GlobalIndex<Partition>();
      gIndex.bulkLoad(selectedSoFar.toArray(new Partition[selectedSoFar.size()]));
      bf.selectCells(gIndex, new ResultCollector<Partition>() {
        @Override
        public void collect(Partition p) {
          selectedSoFar.add(p);
        }
      });
    }
    // Match with whatever selected partitions
    for (Partition p : selectedSoFar)
      output.collect(p);
  }
  
}
