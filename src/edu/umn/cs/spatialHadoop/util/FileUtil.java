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
package edu.umn.cs.spatialHadoop.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * A bunch of helper functions used with files
 * @author Ahmed Eldawy
 *
 */
public final class FileUtil {

  public static String copyFile(Configuration job, FileStatus fileStatus) throws IOException {
    return FileUtil.copyFileSplit(job, new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), new String[0]));
  }

  /**
   * Copies a part of a file from a remote file system (e.g., HDFS) to a local
   * file. Returns a path to a local temporary file.
   * @param conf
   * @param split
   * @return
   * @throws IOException 
   */
  public static String copyFileSplit(Configuration conf, FileSplit split) throws IOException {
    FileSystem fs = split.getPath().getFileSystem(conf);
  
    // Special case of a local file. Skip copying the file
    if (fs instanceof LocalFileSystem && split.getStart() == 0)
      return split.getPath().toUri().getPath();
  
    // Length of input file. We do not depend on split.length because it is not
    // set by input format for performance reason. Setting it in the input
    // format would cost a lot of time because it runs on the client machine
    // while the record reader runs on slave nodes in parallel
    long length = fs.getFileStatus(split.getPath()).getLen();
  
    FSDataInputStream in = fs.open(split.getPath());
    in.seek(split.getStart());
    
    // Prepare output file for write
    File tempFile = File.createTempFile(split.getPath().getName(), "hdf");
    OutputStream out = new FileOutputStream(tempFile);
    
    // A buffer used between source and destination
    byte[] buffer = new byte[1024*1024];
    while (length > 0) {
      int numBytesRead = in.read(buffer, 0, (int)Math.min(length, buffer.length));
      out.write(buffer, 0, numBytesRead);
      length -= numBytesRead;
    }
    
    in.close();
    out.close();
    return tempFile.getAbsolutePath();
  }

  /**
   * Copies a file to the local file system given its path.
   * @param conf
   * @param inFile
   * @return
   * @throws IOException
   */
  public static String copyFile(Configuration conf, Path inFile) throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    return copyFile(conf, fs.getFileStatus(inFile));
  }

}
