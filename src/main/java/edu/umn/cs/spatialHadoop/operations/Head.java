/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import edu.umn.cs.spatialHadoop.indexing.RTree;

/**
 * Reads the first n lines of a text file
 * @author Ahmed Eldawy
 *
 */
public class Head {

  /**
   * Reads a maximum of n lines from the given file.
   * @param fs
   * @param p
   * @param n
   * @return
   * @throws IOException
   */
  public static String[] head(FileSystem fs, Path p,
      int n) throws IOException {
    String[] lines = new String[n];
    FileStatus fstatus = fs.getFileStatus(p);
    
    TaskAttemptContext context = createDummyContext();
    LineRecordReader lineReader = new LineRecordReader();
    FileSplit split;
    if (p.getName().endsWith(".rtree")) {
      // R-tree indexed file
      FSDataInputStream in = fs.open(p);
      in.skip(8); // Skip R-tree signature
      int treeHeaderSize = RTree.skipHeader(in);
      in.close();
      split = new FileSplit(p, treeHeaderSize + 8, fstatus.getLen() - treeHeaderSize - 8, new String[0]);
    } else {
      split = new FileSplit(p, 0, fstatus.getLen(), new String[0]);
    }
    lineReader.initialize(split, context);
    int numOfLines = 0;
    for (numOfLines = 0; numOfLines < lines.length && lineReader.nextKeyValue(); numOfLines++) {
      lines[numOfLines] = lineReader.getCurrentValue().toString();
    }
    lineReader.close();
    
    return lines;
  }

  private static TaskAttemptContext createDummyContext() {
    TaskAttemptID taskId = new TaskAttemptID();
    return new TaskAttemptContextImpl(new Configuration(), taskId);
  }
}
