/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

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
