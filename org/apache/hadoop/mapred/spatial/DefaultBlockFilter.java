package org.apache.hadoop.mapred.spatial;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.GlobalIndex;
import org.apache.hadoop.spatial.Partition;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.ResultCollector2;

/**
 * A default implementation for BlockFilter that returns everything.
 * @author eldawy
 *
 */
public class DefaultBlockFilter implements BlockFilter {
  
  @Override
  public void configure(JobConf job) {
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
