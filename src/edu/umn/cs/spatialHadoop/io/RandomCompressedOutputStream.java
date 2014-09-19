/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
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
  private static final long DefaultBlockSize = 1024 * 1024;
  
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
    DataOutputStream out = new DataOutputStream(new RandomCompressedOutputStream(
        new BufferedOutputStream(new FileOutputStream("test.gzp"))));
    for (int i = 0; i < 1000000; i++) {
      out.writeInt(i);
    }
    out.close();
    
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    RandomCompressedInputStream in = new RandomCompressedInputStream(localFs.open(new Path("test.gzp")),
        new File("test.gzp").length());
    in.seek(4 * 999998);
    DataInputStream din = new DataInputStream(in);
    while (din.available() > 0) {
      System.out.println("Number is "+din.readInt());
    }
    din.close();
  }
}
