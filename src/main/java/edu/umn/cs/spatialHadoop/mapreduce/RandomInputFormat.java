/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.hadoop.mapreduce.*;

public class RandomInputFormat<S extends Shape> extends InputFormat<Rectangle, S> {

  /**The size of each split to produce*/
  public static final String SplitSize = "RandomInputFormat.SplitSize";

  static class GeneratedSplit extends InputSplit {
    
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
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    long totalFileSize = conf.getLongBytes("size", 100);
    long splitSize = conf.getLongBytes(SplitSize, 32 * 1024 * 1024);
    int numSplits = (int) Math.ceil((float) totalFileSize / splitSize);
    List<InputSplit> splits = new ArrayList<InputSplit>();;
    int i;
    for (i = 0; i < numSplits - 1; i++)
      splits.add(new GeneratedSplit(i, splitSize));

    if (totalFileSize % splitSize != 0)
      splits.add(new GeneratedSplit(i, totalFileSize % splitSize));
    else
      splits.add(new GeneratedSplit(i, splitSize));
    return splits;
  }

  @Override
  public RecordReader<Rectangle, S> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
      GeneratedSplit gsplit = (GeneratedSplit) split;
      RandomShapeGenerator rsg = new RandomShapeGenerator<S>();
      rsg.initialize(split, context);
      return rsg;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

}
