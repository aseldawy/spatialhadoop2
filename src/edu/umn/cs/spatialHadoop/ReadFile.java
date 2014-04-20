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
package edu.umn.cs.spatialHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * Reads spatial information associated with a file
 * @author eldawy
 *
 */
public class ReadFile {

  private static void printUsage() {
    System.out.println("Displays information about blocks in a file");
    System.out.println("Parameters:");
    System.out.println("<input file> - Path to input file");
  }
  
  public static void main(String[] args) throws Exception {
    OperationsParams cla = new OperationsParams(new GenericOptionsParser(args));
    Path input = cla.getPath();
    if (input == null) {
      printUsage();
      throw new RuntimeException("Illegal parameters");
    }
    Configuration conf = new Configuration();
    Path inFile = new Path(args[0]);
    FileSystem fs = inFile.getFileSystem(conf);
    
    long length = fs.getFileStatus(inFile).getLen();
    
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, inFile);
    if (gindex == null) {
      BlockLocation[] locations = cla.getInt("offset", 0) == -1 ?
          fs.getFileBlockLocations(fs.getFileStatus(inFile), 0, length) :
            fs.getFileBlockLocations(fs.getFileStatus(inFile), cla.getInt("offset", 0), 1);
      System.out.println(locations.length+" heap blocks");
    } else {
      for (Partition p : gindex) {
        long partition_length = fs.getFileStatus(new Path(inFile, p.filename)).getLen();
        System.out.println(p+" --- "+partition_length);
      }
    }
  }
}
