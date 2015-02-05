/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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
  private int currentBlock;

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
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }
  
  private void gotoBlock(int blockIndex) throws IOException {
    compressedIn.seek(this.blockOffsetsInCompressedFile[blockIndex]);
    long blockSizeInCompressedFile = this.blockOffsetsInCompressedFile[blockIndex+1] - this.blockOffsetsInCompressedFile[blockIndex];
    decompressedIn = new GZIPInputStream(new InputSubstream(this.compressedIn, blockSizeInCompressedFile));
    this.pos = this.blockOffsetsInRawFile[blockIndex];
    this.currentBlock = blockIndex;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos >= getPos() && newPos < this.blockOffsetsInRawFile[currentBlock + 1]) {
      // Just skip bytes to newPos if within the same block
      this.skip(newPos - getPos());
      return;
    }
    int newBlock = findBlock(newPos);
    gotoBlock(newBlock);
    this.skip(newPos - getPos());
  }

  private boolean gotoBlockNewSource(int blockIndex) throws IOException {
    if (!compressedIn.seekToNewSource(this.blockOffsetsInCompressedFile[blockIndex]))
      return false;
    long remainingCompressedBytes = getCompressedLength() - this.blockOffsetsInCompressedFile[blockIndex];
    decompressedIn = new GZIPInputStream(new InputSubstream(this.compressedIn, remainingCompressedBytes));
    this.pos = this.blockOffsetsInRawFile[blockIndex];
    this.currentBlock = blockIndex;
    return true;
  }
  
  private long getCompressedLength() {
    return blockOffsetsInCompressedFile[blockOffsetsInCompressedFile.length - 1];
  }

  private long getDecompressedLength() {
    return blockOffsetsInRawFile[blockOffsetsInRawFile.length - 1];
  }

  @Override
  public boolean seekToNewSource(long newPos) throws IOException {
    int newBlock = findBlock(newPos);
    if (!gotoBlockNewSource(newBlock))
      return false;
    this.skip(newPos - this.blockOffsetsInRawFile[newBlock]);
    return true;
  }

  @Override
  public int read() throws IOException {
    if (pos >= getDecompressedLength())
      return -1;
    int b = this.decompressedIn.read();
    pos++;
    if (pos >= blockOffsetsInRawFile[currentBlock+1]) {
      currentBlock++;
    }
    return b;
  }
  
  @Override
  public long skip(long n) throws IOException {
    long canSkip = Math.min(n, getDecompressedLength() - getPos());
    this.decompressedIn.skip(canSkip);
    pos += canSkip;
    return canSkip;
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

  @Override
  public int available() throws IOException {
    return (int) Math.min(getDecompressedLength() - pos, this.compressedIn.available());
  }
}
