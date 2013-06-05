package org.apache.hadoop.mapred.spatial;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.GlobalIndex;
import org.apache.hadoop.spatial.Partition;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.ResultCollector2;

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
  public void configure(JobConf job);

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
