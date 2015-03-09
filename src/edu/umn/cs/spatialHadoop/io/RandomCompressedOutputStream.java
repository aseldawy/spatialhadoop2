/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An output stream that writes data in separate blocks each one is compressed
 * separately using gzip. It includes a lookup table that allows pseudo random
 * access to the file.
 * 
 * @author Ahmed Eldawy
 *
 */
public class RandomCompressedOutputStream extends OutputStream {
  /**Default size for one block to be compressed separately*/
  private static final long DefaultBlockSize = 10 * 1024 * 1024;
  
  /**The output stream on which raw data is written (internally compressed)*/
  private GZIPOutputStream rawOut;
  
  /**The output stream to which compressed bytes are written*/
  private TrackedOutputStream compressedOut;
  
  /**Current offset in the space of uncompressed data*/
  private long rawOffset;
  
  /**Number of raw bytes to include in each compressed block*/
  private long blockSize;
  
  /**The raw offset on which the last block was written*/
  private long rawOffsetOfLastBlock;
  
  private Vector<Long> blockOffsetsInCompressedFile;
  private Vector<Long> blockOffsetsInRawFile;

  public RandomCompressedOutputStream(OutputStream out) throws IOException {
    // out is the OutputStream to which compressed data is written.
    this.compressedOut = new TrackedOutputStream(out);
    this.rawOut = new GZIPOutputStream(this.compressedOut);
    this.blockSize = DefaultBlockSize;
    this.blockOffsetsInCompressedFile = new Vector<Long>();
    this.blockOffsetsInRawFile = new Vector<Long>();
  }

  @Override
  public void write(int b) throws IOException {
    this.rawOut.write(b);
    rawOffset++;
    
    if (rawOffset - rawOffsetOfLastBlock >= blockSize) {
      finishCurrentBlock();
      // Start a new block
      this.rawOut = new GZIPOutputStream(this.compressedOut);
    }
  }
  
  @Override
  public void close() throws IOException {
    this.finishCurrentBlock();
    // Store the lookup table at the end of the stream in uncompressed format
    DataOutputStream dout = new DataOutputStream(this.compressedOut);
    for (int i = 0; i < blockOffsetsInCompressedFile.size(); i++) {
      dout.writeLong(blockOffsetsInCompressedFile.get(i));
      dout.writeLong(blockOffsetsInRawFile.get(i));
    }
    dout.writeInt(blockOffsetsInCompressedFile.size());
    dout.close();
  }
  
  private void finishCurrentBlock() throws IOException {
    // Write all the data to out and start a new block
    this.rawOut.finish();
    this.rawOut.flush();
    this.compressedOut.flush();
    // Save the current checkpoint
    long compressedOffset = this.compressedOut.getPos();
    this.blockOffsetsInCompressedFile.add(compressedOffset);
    this.blockOffsetsInRawFile.add(rawOffset);
    this.rawOffsetOfLastBlock = rawOffset;
  }

  public static void main(String[] args) throws IOException {
    long t1 = System.currentTimeMillis();
    DataOutputStream out = new DataOutputStream(new RandomCompressedOutputStream(
        new BufferedOutputStream(new FileOutputStream("test.gzp"))));
    for (int i = 0; i < 10000000; i++) {
      out.writeInt(i);
    }
    out.close();
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for writing the file: "+(t2-t1)/1000.0+" secs");

    FileSystem localFs = FileSystem.getLocal(new Configuration());
    t1 = System.currentTimeMillis();
    InputStream in = new RandomCompressedInputStream(localFs, new Path("test.gzp"));
    FSDataInputStream din = new FSDataInputStream(in);
    long[] pos = new long[1000];
    Random rand = new Random();
    for (int i = 0; i < pos.length; i++) {
      pos[i] = rand.nextInt(10000000) * 4L;
    }
    Arrays.sort(pos);
    for (int i = 0; i < pos.length; i++) {
      //din.seek(pos[i]);
      din.skip(pos[i] - din.getPos());
      din.readInt();
      //System.out.println("Number is "+din.readInt());
    }
    t2 = System.currentTimeMillis();
    System.out.println("Total time for reading the file: "+(t2-t1)/1000.0+" secs");
    din.close();
  }
}
