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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

/**
 * An operation to read a random sample from a file or a set of files.
 * @author Ahmed Eldawy
 *
 */
public class Sampler2 {

  /**
   * Read a random sample of up-to count from the input files.
   * @param files
   * @param count
   * @param output
   * @param conf
   * @return
   * @throws IOException
   * @throws InterruptedException 
   */
  public static long sampleLocalByCount(Path[] files, int count,
      ResultCollector<Text> output, OperationsParams conf) throws IOException, InterruptedException {
    Vector<FileSplit> splits = new Vector<FileSplit>();
    for (Path file : files) {
      FileSystem fs = file.getFileSystem(conf);
      if (fs.isFile(file)) {
        // A single file. Include it
        splits.add(new FileSplit(file, 0, fs.getFileStatus(file).getLen(), new String[0]));
      } else {
        // A directory. Include all contents
        FileStatus[] contents = fs.listStatus(file);
        for (FileStatus content : contents) {
          if (!content.isDir())
            splits.add(new FileSplit(content.getPath(), 0, content.getLen(), new String[0]));
        }
      }
    }
    
    return sampleLocalByCount(splits.toArray(new FileSplit[splits.size()]), count, output, conf);
  }
  
  /**
   * Reads a random sample of up-to count from the given set of file splits.
   * @param paths
   * @param count
   * @param output
   * @param conf
   * @return - the actual number of lines read from the file
   * @throws IOException
   * @throws InterruptedException 
   */
  public static long sampleLocalByCount(final FileSplit[] files, int count,
      final ResultCollector<Text> output, final OperationsParams conf) throws IOException, InterruptedException {
    // A prefix sum of all files sizes. Used to draw a different sample size
    // from each file according to its size
    long[] fileStartOffset = new long[files.length + 1];
    fileStartOffset[0] = 0;
    for (int i = 0; i < files.length; i++)
      fileStartOffset[i+1] = fileStartOffset[i] + files[i].getLength();

    // Decide number of samples to read from each file according to its size
    final int[] sampleSizePerFile = new int[files.length];
    Random rand = new Random(conf.getLong("seed", System.currentTimeMillis()));
    for (int i = 0; i < count; i++) {
      long sampleOffset = Math.abs(rand.nextLong()) % fileStartOffset[files.length];
      int iFile = Arrays.binarySearch(fileStartOffset, sampleOffset);
      // An offset in the middle of a file.
      if (iFile < 0)
        iFile = -iFile - 1 - 1;
      sampleSizePerFile[iFile]++;
    }
    
    Vector<Integer> actualSampleSizes = Parallel.forEach(files.length, new RunnableRange<Integer>() {
      @Override
      public Integer run(int i1, int i2) {
        int sampledLines;
        Text line = new Text2();
        sampledLines = 0;
        for (int iFile = i1; iFile < i2; iFile++) {
          try {
            // Open the file and read the sample
            FileSystem fs = files[iFile].getPath().getFileSystem(conf);
            InputStream in = fs.open(files[iFile].getPath());
            long pos = 0; // Current position in file

            Random rand = new Random(iFile + conf.getLong("seed", System.currentTimeMillis()));
            for (int i = 0; i < sampleSizePerFile[iFile]; i++) {
              long randomOffset = Math.abs(rand.nextLong()) % files[iFile].getLength() + files[iFile].getStart();

              pos += in.skip(randomOffset - pos);
              // Skip until end of line
              line.clear();
              pos += readUntilEOL(in, line);
              // Read the next full line
              line.clear();
              if ((pos += readUntilEOL(in, line)) > 1) {
                sampledLines++;
                if (output != null)
                  output.collect(line);
              }

            }

            in.close();
          } catch (IOException e) {
            throw new RuntimeException("Error while sampling file "+files[iFile]);
          }
        }
        return sampledLines;
      }
    });
    
    int totalSampledLines = 0;
    for (int actualSampleSize : actualSampleSizes)
      totalSampledLines += actualSampleSize;
    return totalSampledLines;
  }

  /**
   * Read from the given stream until end-of-line is reached.
   * @param in - the input stream from where to read the line
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
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
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

    long t1 = System.currentTimeMillis();
    long lines = sampleLocalByCount(inputFiles, count, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Sampled "+lines+" lines in "+(t2-t1)+" millis");
  }

}
