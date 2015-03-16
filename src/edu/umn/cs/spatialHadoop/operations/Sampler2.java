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
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.io.Text2;

/**
 * An operation to read a random sample from a file or a set of files.
 * @author Ahmed Eldawy
 *
 */
public class Sampler2 {

  /***
   * Read a random sample of up-to count from the input files.
   * @param paths
   * @param count
   * @param output
   * @param conf
   * @return - the actual number of lines read from the file
   * @throws IOException
   */
  public static long sampleLocalByCount(Path[] paths, int count,
      ResultCollector<Text> output, OperationsParams conf) throws IOException {
    // A prefix sum of all files sizes
    Vector<Long> fileStartOffset = new Vector<Long>();
    Vector<FileStatus> filesToReadFrom = new Vector<FileStatus>();
    long overallSize = 0;
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(conf);
      if (OperationsParams.isWildcard(path)) {
        FileStatus[] matchingFiles = fs.globStatus(path);
        for (FileStatus fileStatus : matchingFiles) {
          fileStartOffset.add(overallSize);
          overallSize += fileStatus.getLen();
          filesToReadFrom.add(fileStatus);
        }
      } else {
        if (fs.isFile(path)) {
          FileStatus matchingFile = fs.getFileStatus(path);
          fileStartOffset.add(overallSize);
          overallSize = matchingFile.getLen();
          filesToReadFrom.add(matchingFile);
        } else if (fs.isDirectory(path)) {
          FileStatus[] dirContents = fs.listStatus(path);
          for (FileStatus matchingFile : dirContents) {
            fileStartOffset.add(overallSize);
            overallSize = matchingFile.getLen();
            filesToReadFrom.add(matchingFile);
          }
        }
      }
    }
    fileStartOffset.add(overallSize);
    
    // Generate offsets to sample
    Random rand = new Random();
    long[] offsets = new long[count];
    for (int iSample = 0; iSample < offsets.length; iSample++)
      offsets[iSample] = Math.abs(rand.nextLong()) % overallSize;
    Arrays.sort(offsets);
    
    Text line = new Text2();
    // Read all locations of these offsets
    int iFile = 0; // The index of the file to process
    int iSample = 0;
    long sampledLines = 0;
    while (iSample < offsets.length && iSample < offsets[iSample]) {
      // Skip until the file that contains this offset
      while (fileStartOffset.get(iFile+1) <= offsets[iSample])
        iFile++;
      
      FileSystem fs = filesToReadFrom.get(iFile).getPath().getFileSystem(conf);
      FSDataInputStream in = fs.open(filesToReadFrom.get(iFile).getPath());
      
      // Read all random samples in this file for more efficiency
      while (iSample < offsets.length && offsets[iSample] < fileStartOffset.get(iFile+1)) {
        in.seek(offsets[iSample] - fileStartOffset.get(iFile));
        // Skip until end of line
        line.clear();
        readUntilEOL(in, line);
        // Read the next full line
        line.clear();
        if (readUntilEOL(in, line) > 1) {
          sampledLines++;
          if (output != null)
            output.collect(line);
        }
        
        iSample++;
      }
      in.close();
    }
    return sampledLines;
  }

  /**
   * Read from the given stream until end-of-line is reached.
   * @param in - the input stream from where to read the ine
   * @param line - the line that has been read from file not including EOL
   * @return - number of bytes read including EOL characters
   * @throws IOException 
   */
  private static int readUntilEOL(InputStream in, Text line) throws IOException {
    final byte[] lineBytes = new byte[1024];
    int length = 0;
    do {
      if (length == lineBytes.length) {
        line.append(lineBytes, 0, length);
        length = 0;
      }
      if (length == 0) {
        // Read and skip any initial EOL characters
        do {
          lineBytes[0] = (byte) in.read();
        } while (lineBytes[0] != -1 &&
            (lineBytes[0] == '\n' || lineBytes[0] == '\r'));
        if (lineBytes[0] != -1)
          length++;
      } else {
        lineBytes[length++] = (byte) in.read();
      }
    } while (lineBytes[length-1] != -1 &&
        lineBytes[length-1] != '\n' && lineBytes[length-1] != '\r');
    length--;
    line.append(lineBytes, 0, length);
    return line.getLength();
  }

  private static void printUsage() {
    System.out.println("Reads a random sample of an input file. Sample is written to stdout");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("shape:<s> - Type of shapes stored in the file");
    System.out.println("outshape:<s> - Shapes to write to output");
    System.out.println("ratio:<r> - ratio of random sample to read [0, 1]");
    System.out.println("count:<s> - approximate number of records in the sample");
    System.out.println("size:<s> - approximate size of the sample in bytes");
    System.out.println("seed:<s> - random seed to use while reading the sample");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] inputFiles = params.getPaths();
    
    if (!params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    
    ResultCollector<Text> output = new ResultCollector<Text>() {
      @Override
      public void collect(Text value) {
        System.out.println(value);
      }
    };
    
    int count = params.getInt("count", 0);

    long lines = sampleLocalByCount(inputFiles, count, output, params);
    System.out.println("Sampled "+lines+" lines");
  }

}
