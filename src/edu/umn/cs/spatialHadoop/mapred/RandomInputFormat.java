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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

public class RandomInputFormat<S extends Shape> implements InputFormat<Rectangle, S> {

  static class GeneratedSplit implements InputSplit {
    
    /**Index of this split*/
    int index;
    
    /**Length of this split*/
    long length;
    
    public GeneratedSplit() {}
    
    public GeneratedSplit(int index, long length) {
      super();
      this.index = index;
      this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(index);
      out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.index = in.readInt();
      this.length = in.readLong();
    }

    @Override
    public long getLength() throws IOException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
      final String[] emptyLocations = new String[0];
      return emptyLocations;
    }
  }

  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    long totalFileSize = OperationsParams.getSize(job, "size");
    long splitSize = FileSystem.get(job).getDefaultBlockSize(new Path("/"));
    InputSplit[] splits = new InputSplit[(int) Math.ceil((double)totalFileSize / splitSize)];
    int i;
    for (i = 0; i < splits.length - 1; i++) {
      splits[i] = new GeneratedSplit(i, splitSize);
    }
    if (totalFileSize % splitSize != 0)
      splits[i] = new GeneratedSplit(i, totalFileSize % splitSize);
    else
      splits[i] = new GeneratedSplit(i, splitSize);
    return splits;
  }

  @Override
  public RecordReader<Rectangle, S> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    GeneratedSplit gsplit = (GeneratedSplit) split;
    return new RandomShapeGenerator<S>(job, gsplit);
  }


}
