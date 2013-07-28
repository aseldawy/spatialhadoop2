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
