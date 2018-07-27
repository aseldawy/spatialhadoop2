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

import edu.umn.cs.spatialHadoop.core.SpatialSite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;


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
  public static String[] head(FileSystem fs, Path p, int n) throws IOException {
    String[] lines = new String[n];
    FileStatus fstatus = fs.getFileStatus(p);
    
    TaskAttemptContext context = createDummyContext();
    LineRecordReader lineReader = new LineRecordReader();
    FileSplit[] splits;
    if (fstatus.isFile()) {
      splits = new FileSplit[] {
          new FileSplit(p, 0, fstatus.getLen(), new String[0])
      };
    } else {
      FileStatus[] files = fs.listStatus(p, SpatialSite.NonHiddenFileFilter);
      splits = new FileSplit[files.length];
      for (int i = 0; i < files.length; i++)
        splits[i] = new FileSplit(files[i].getPath(), 0, files[i].getLen(), new String[0]);
    }
    int iLine = 0;
    int iSplit = 0;
    while (iLine < lines.length && iSplit < splits.length) {
      lineReader.initialize(splits[iSplit], context);
      while (iLine < lines.length && lineReader.nextKeyValue()) {
        lines[iLine++] = lineReader.getCurrentValue().toString();
      }
      lineReader.close();
      iSplit++;
    }

    if (iLine < lines.length) {
      // The input path contained less than the maximum required size
      // Srhink the result array to eliminate any nulls at the end.
      String[] resizedArray = new String[iLine];
      System.arraycopy(lines, 0, resizedArray, 0, iLine);
      lines = resizedArray;
    }

    return lines;
  }

  private static TaskAttemptContext createDummyContext() {
    TaskAttemptID taskId = new TaskAttemptID();
    return new TaskAttemptContextImpl(new Configuration(), taskId);
  }
}
