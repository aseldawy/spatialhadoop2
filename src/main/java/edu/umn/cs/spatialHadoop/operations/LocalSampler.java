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
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
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
public class LocalSampler {

  /**
   * Read a random sample of up-to count from the input files.
   * @param files
   * @param ratioOrCount
   * @param output
   * @param conf
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static long sampleLocal(Path[] files, float ratioOrCount,
      ResultCollector<Text> output, Configuration conf) throws IOException, InterruptedException {
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
          if (!content.isDirectory())
            splits.add(new FileSplit(content.getPath(), 0, content.getLen(), new String[0]));
        }
      }
    }
    return sampleLocal(splits.toArray(new FileSplit[splits.size()]), ratioOrCount, output, conf);
  }
  
  /**
   * Reads a random sample of up-to count from the given set of file splits.
   * @param files
   * @param ratioOrCount
   * @param output
   * @param conf
   * @return the actual number of lines read from the file
   * @throws IOException
   * @throws InterruptedException
   */
  public static long sampleLocal(final FileSplit[] files, final float ratioOrCount,
      final ResultCollector<Text> output, final Configuration conf) throws IOException, InterruptedException {
    // A prefix sum of all files sizes. Used to draw a different sample size
    // from each file according to its size
    long[] fileStartOffset = new long[files.length + 1];
    fileStartOffset[0] = 0;
    for (int i = 0; i < files.length; i++)
      fileStartOffset[i+1] = fileStartOffset[i] + files[i].getLength();

    // Decide number of samples to read from each file according to its size
    final int[] sampleSizePerFile = new int[files.length];
    Random rand = new Random(conf.getLong("seed", System.currentTimeMillis()));
    
    if (ratioOrCount > 1) {
      // This indicates a count
      for (int i = 0; i < ratioOrCount; i++) {
        long sampleOffset = Math.abs(rand.nextLong()) % fileStartOffset[files.length];
        int iFile = Arrays.binarySearch(fileStartOffset, sampleOffset);
        // An offset in the middle of a file.
        if (iFile < 0)
          iFile = -iFile - 1 - 1;
        sampleSizePerFile[iFile]++;
      }
    }
    
    List<Integer> actualSampleSizes = Parallel.forEach(files.length, new RunnableRange<Integer>() {
      @Override
      public Integer run(int i1, int i2) {
        int sampledLines;
        
        sampledLines = 0;
        for (int iFile = i1; iFile < i2; iFile++) {
          try {
            long randomSeed = conf.getLong("seed", System.currentTimeMillis()) + iFile;
            if (ratioOrCount > 1)
              sampledLines += sampleFileSplitByCount(files[iFile], conf,
                  sampleSizePerFile[iFile], randomSeed, output);
            else
                  sampledLines += sampleFileSplitByRatio(files[iFile], conf,
                      ratioOrCount, randomSeed, output);
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
   * Sample from a stream by a sampling ratio. The stream has to be scanned
   * and each record is selected with the a probability equal to the given
   * sampling ratio.
   * @param in
   * @param streamLength
   * @param ratio
   * @param seed
   * @param output
   * @return
   * @throws IOException
   */
  private static int sampleStreamByRatio(InputStream in,
      double ratio, long seed, ResultCollector<Text> output) throws IOException {
    Random rand = new Random(seed);
    Text line = new Text2();
    int sampleSize = 0;
    while (readUntilEOL(in, line) > 0) {
      if (rand.nextDouble() < ratio) {
        if (output != null)
          output.collect(line);
        sampleSize++;
      }
      line.clear();
    }
    
    return sampleSize;
  }

  /**
   * Reads a sample of a specific count from a stream. With a stream, we cannot
   * randomly seek to any position and we have to use reservoir sampling
   * techniques.
   * 
   * @return
   * @throws IOException
   */
  private static int sampleStreamByCount(InputStream in, long streamLength,
      int count, long seed, ResultCollector<Text> output) throws IOException {
    Random rand = new Random(seed);
    
    Text dummyLine = new Text2();
    Text[] sample = new Text[count];
    long pos = 0;
    int k = 0;
    
    while (pos < streamLength) {
      if (k < count) {
        // Phase 1- Fill in the reservoir
        pos += readUntilEOL(in, sample[k] = new Text2());
      } else {
        // Phase 2- Replace an existing item with probability p=(count/k)
        if (rand.nextInt(k) < count) {
          // Replace a randomly selected item
          int victim = rand.nextInt(count);
          sample[victim].clear();
          pos += readUntilEOL(in, sample[victim]);
        } else {
          // Skip this item
          dummyLine.clear();
          pos += readUntilEOL(in, dummyLine);
        }
      }
      k++;
    }
    // Report sampled items
    int sampleSize = Math.min(k, count);
    if (output != null) {
      for (int i = 0; i < sampleSize; i++)
        output.collect(sample[i]);
    }
    
    return sampleSize;
  }  
  
  /**
   * Sample a specific number of lines from a given file
   * @param fs
   * @param file
   * @param count
   * @param seed
   * @param output
   * @return
   * @throws IOException
   */
  private static int sampleFileSplitByCount(FileSplit file, Configuration conf,
      int count, long seed, ResultCollector<Text> output) throws IOException {
    InputStream in = null;
    Decompressor decompressor = null;
    try {
      CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecFactory.getCodec(file.getPath());
      
      // Open the file and read the sample
      FileSystem fs = file.getPath().getFileSystem(conf);
      in = fs.open(file.getPath());
      int sampledLines = 0;
      
      if (codec != null) {
        // Special handling for compressed file as we cannot compute the actual
        // size of the underlying data
        decompressor = CodecPool.getDecompressor(codec);
        
        if (codec instanceof SplittableCompressionCodec) {
          // A splittable compression codec, can seek to the desired input pos
          final SplitCompressionInputStream cIn =
              ((SplittableCompressionCodec)codec).createInputStream(
                  in, decompressor, file.getStart(), file.getStart() + file.getLength(),
                  SplittableCompressionCodec.READ_MODE.BYBLOCK);
          in = cIn;
          // Adjust the start of the end based on the compressed data
          long start = cIn.getAdjustedStart();
          long end = cIn.getAdjustedEnd();
          sampledLines = sampleStreamByCount(in, end - start, count, seed, output);
        } else {
          // Non-splittable input, need to start from the beginning
          in = codec.createInputStream(in, decompressor);
          sampledLines = sampleStreamByCount(in, Long.MAX_VALUE, count, seed, output);
        }
      } else {
        long pos = 0; // Current position in file
        
        // Generate random offsets and keep them sorted for IO efficiency
        Random rand = new Random(seed);
        long[] sampleOffsets = new long[count];
        for (int i = 0; i < count; i++)
          sampleOffsets[i] = Math.abs(rand.nextLong()) % file.getLength() + file.getStart();
        Arrays.sort(sampleOffsets);
        
        // Sample the generated numbers
        Text line = new Text2();
        for (int i = 0; i < count; i++) {
          pos += in.skip(sampleOffsets[i] - pos);
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
      }
      
      return sampledLines;
    } finally {
      if (in != null)
        in.close();
      if (decompressor != null)
        CodecPool.returnDecompressor(decompressor);
    }
  }
  
  /**
   * Sample text lines from the given split with the given sampling ratio
   * @param fs
   * @param file
   * @param ratio
   * @param seed
   * @param output
   * @return
   * @throws IOException
   */
  private static int sampleFileSplitByRatio(FileSplit file,
      Configuration conf,
      float ratio, long seed, ResultCollector<Text> output) throws IOException {
    
    InputStream in = null;
    Decompressor decompressor = null;
    int sampledLines;
    Text line = new Text2();
    
    try {
      CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecFactory.getCodec(file.getPath());
      FileSystem fs = file.getPath().getFileSystem(conf);
      in = fs.open(file.getPath());
      
      if (codec != null) {
        // Special handling for compressed file as we cannot compute the actual
        // size of the underlying data
        decompressor = CodecPool.getDecompressor(codec);
        
        if (codec instanceof SplittableCompressionCodec) {
          // A splittable compression codec, can seek to the desired input pos
          final SplitCompressionInputStream cIn =
              ((SplittableCompressionCodec)codec).createInputStream(
                  in, decompressor, file.getStart(), file.getStart() + file.getLength(),
                  SplittableCompressionCodec.READ_MODE.BYBLOCK);
          in = cIn;
          // Adjust the start of the end based on the compressed data
          long start = cIn.getAdjustedStart();
          long end = cIn.getAdjustedEnd();
          // Skip first line if needed
          if (file.getStart() > 0)
            start += readUntilEOL(cIn, line);
          
          sampledLines = sampleStreamByRatio(in, ratio, seed, output);
        } else {
          // Non-splittable input, need to start from the beginning
          in = codec.createInputStream(in, decompressor);
          // No need to skip first line because we actually read the file from
          // the beginning
          sampledLines = sampleStreamByRatio(in, ratio, seed, output);
        }
      } else {
        // Not a compressed file. Apply a more efficient, though approximate,
        // solution
        // Open the file and read the sample
        long pos = 0; // Current position in file
        if (file.getStart() > 0) {
          pos += in.skip(file.getStart());
          pos += readUntilEOL(in, line);
        }
        
        // Initialize the random variable which is used for sampling
        Random rand = new Random(seed);
        sampledLines = 0;

        // Read the first 10 lines to estimate the average record size
        long end = file.getStart() + file.getLength();
        for (int i = 0; i < 10 && pos < end; i++) {
          line.clear();
          pos += readUntilEOL(in, line);
          if (rand.nextFloat() < ratio) {
            sampledLines++;
            if (output != null)
              output.collect(line);
          }
        }
        
        int averageLineSize = (int) ((pos - file.getStart()) / 10);
        int count = Math.round(ratio * file.getLength() / averageLineSize) - sampledLines;
        long[] sampleOffsets = new long[count];
        for (int i = 0; i < count; i++)
          sampleOffsets[i] = Math.abs(rand.nextLong()) % (end - pos) + file.getStart();
        Arrays.sort(sampleOffsets);

        // Sample the generated numbers
        for (int i = 0; i < count; i++) {
          pos += in.skip(sampleOffsets[i] - pos);
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
      }
    } finally {
      if (in != null)
        in.close();
      if (decompressor != null)
        CodecPool.returnDecompressor(decompressor);
    }

    in.close();
    return sampledLines;
  }


  /**
   * Read from the given stream until end-of-line is reached.
   * @param in - the input stream from where to read the line
   * @param line - the line that has been read from file not including EOL
   * @return - number of bytes read including EOL characters
   * @throws IOException 
   */
  public static int readUntilEOL(InputStream in, Text line) throws IOException {
    final byte[] bufferBytes = new byte[1024];
    int bufferLength = 0; // Length of the buffer
    do {
      if (bufferLength == bufferBytes.length) {
        // Buffer full. Copy to the output text
        line.append(bufferBytes, 0, bufferLength);
        bufferLength = 0;
      }
      if (bufferLength == 0) {
        // Read and skip any initial EOL characters
        do {
          bufferBytes[0] = (byte) in.read();
        } while (bufferBytes[0] != -1 &&
            (bufferBytes[0] == '\n' || bufferBytes[0] == '\r'));
        if (bufferBytes[0] != -1)
          bufferLength++;
      } else {
        bufferBytes[bufferLength++] = (byte) in.read();
      }
    } while (bufferLength > 0 &&
        bufferBytes[bufferLength-1] != -1 &&
        bufferBytes[bufferLength-1] != '\n' && bufferBytes[bufferLength-1] != '\r');
    if (bufferLength > 0) {
      bufferLength--;
      line.append(bufferBytes, 0, bufferLength);
    }
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
    
    float sampleRatioOrCount = params.getFloat("ratio", params.getInt("count", 0));

    long t1 = System.currentTimeMillis();
    long lines = sampleLocal(inputFiles, sampleRatioOrCount, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Sampled "+lines+" lines in "+(t2-t1)+" millis");
  }

}
