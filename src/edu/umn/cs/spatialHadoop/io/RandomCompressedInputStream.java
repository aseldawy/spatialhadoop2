/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * Provides random access to a file that was previously written using
 * {@link RandomCompressedOutputStream}. The underlying stream should be
 * seekable and the total size of it should be known because the lookup table
 * that helps doing the random access is stored at the very end.
 * 
 * @author Ahmed Eldawy
 *
 */
public class RandomCompressedInputStream extends InputStream implements Seekable, PositionedReadable {
  /**The underlying stream of compressed data*/
  private FSDataInputStream compressedIn;
  private InputStream decompressedIn;

  private long[] blockOffsetsInCompressedFile;
  private long[] blockOffsetsInRawFile;
  
  private long pos;
  /**Number of bytes in the decompressed file*/
  private long decompressedLength;

  public RandomCompressedInputStream(FileSystem fs, Path p) throws IOException {
    this(fs.open(p), fs.getFileStatus(p).getLen());
  }
  
  public RandomCompressedInputStream(FSDataInputStream in, long totalLength) throws IOException {
    this.compressedIn = new FSDataInputStream(in);
    // Read and cache the lookup table
    this.compressedIn.seek(totalLength - 4);
    int numberOfBlocks = this.compressedIn.readInt();
    this.blockOffsetsInCompressedFile = new long[numberOfBlocks + 1];
    this.blockOffsetsInRawFile = new long[numberOfBlocks + 1];
    this.compressedIn.seek(totalLength - 4 - numberOfBlocks * (8 + 8));
    for (int i = 1; i <= numberOfBlocks; i++) {
      blockOffsetsInCompressedFile[i] = this.compressedIn.readLong();
      blockOffsetsInRawFile[i] = this.compressedIn.readLong();
    }
    this.compressedIn.seek(0);
    this.decompressedIn = new GZIPInputStream(this.compressedIn);
    this.decompressedLength = blockOffsetsInRawFile[numberOfBlocks - 1];
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    int blockIndex = findBlock(newPos);
    compressedIn.seek(this.blockOffsetsInCompressedFile[blockIndex]);
    this.decompressedIn = new GZIPInputStream(this.compressedIn);
    this.pos = this.blockOffsetsInRawFile[blockIndex];
    this.decompressedIn.skip(newPos - this.blockOffsetsInRawFile[blockIndex]);
  }

  @Override
  public boolean seekToNewSource(long newPos) throws IOException {
    int blockIndex = findBlock(newPos);
    if (!compressedIn.seekToNewSource(this.blockOffsetsInCompressedFile[blockIndex])) {
      return false;
    }
    this.decompressedIn = new GZIPInputStream(this.compressedIn);
    this.pos = this.blockOffsetsInRawFile[blockIndex];
    this.decompressedIn.skip(newPos - this.blockOffsetsInRawFile[blockIndex]);
    return true;
  }

  @Override
  public int read() throws IOException {
    if (pos > decompressedLength)
      return -1;
    int b = this.decompressedIn.read();
    pos++;
    return b;
  }
  
  @Override
  public void close() throws IOException {
    this.compressedIn.close();
  }
  
  /**
   * Finds the block that contains the given position in the uncompressed
   * file.
   * @param newPos
   * @return
   */
  private int findBlock(long newPos) {
    int s = 0;
    int e = blockOffsetsInRawFile.length;
    while (s < e) {
      int m = (s + e) / 2;
      if (blockOffsetsInRawFile[m] < newPos) {
        s = m + 1;
      } else {
        e = m;
      }
    }
    return s - 1;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long oldPos = getPos();
    seek(position);
    int x = read(buffer, offset, length);
    seek(oldPos);
    return x;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long oldPos = getPos();
    seek(position);
    read(buffer, offset, length);
    seek(oldPos);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

}
