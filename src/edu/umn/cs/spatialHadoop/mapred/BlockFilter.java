/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.mapred;

import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;

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
