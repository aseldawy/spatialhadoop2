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

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A record writer to be used to write the output of MapReduce programs to
 * a spatial index. The key is a rectangle which indicates the MBR of the
 * partition and value is a shape. This record writer does not replicate the
 * given shape to partition (i.e., write it only to the given partition). If
 * the provided rectangle (key) does not match any of the existing partitions,
 * a new partition is created with the given boundaries.
 * @author Ahmed Eldawy
 *
 * @param <S>
 */
public class GridRecordWriter3<S extends Shape>
extends edu.umn.cs.spatialHadoop.core.GridRecordWriter<S> implements RecordWriter<Rectangle, S> {

  public GridRecordWriter3(JobConf job, String name, CellInfo[] cells) throws IOException {
    super(null, job, name, cells);
  }
  
  @Override
  public void write(Rectangle key, S value) throws IOException {
    
    super.write(key, value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
