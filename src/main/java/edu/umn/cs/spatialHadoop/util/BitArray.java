/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

/**
 * An array of bits which is stored efficiently in memory and can be serialized
 * or deserialized using Hadoop serialization framework.
 * @author Ahmed Eldawy
 *
 */
public class BitArray implements Writable {

  /**Number of bits per entry*/
  private static final int BitsPerEntry = 64;
  
  /**Condensed representation of all data*/
  protected long[] entries;
  
  /**Total number of bits stores in this array*/
  protected long size;

  /**Default constructor is needed for deserialization*/
  public BitArray() {
  }
  
  /**
   * Initializes a bit array with the given capacity in bits.
   * @param size Total number of bits in the array. All initialized to false.
   */
  public BitArray(long size) {
    this.size = size;
    entries = new long[(int) ((size + BitsPerEntry - 1) / BitsPerEntry)];
  }

  @Override
  public BitArray clone() {
    BitArray replica = new BitArray();
    replica.size = this.size;
    replica.entries = this.entries.clone();
    return replica;
  }

  /**
   * Sets the bit at position <code>i</code>
   * @param i
   * @param b
   */
  public void set(long i, boolean b) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    if (b) {
      entries[entry] |= (1L << offset);
    } else {
      entries[entry] &= ~(1L << offset);
    }
  }

  /**
   * Resize the array to have at least the given new size without losing the
   * current data
   * @param newSize
   */
  public void resize(long newSize) {
    if (newSize > size) {
      // Resize needed
      int newArraySize = (int) ((newSize + BitsPerEntry - 1) / BitsPerEntry);
      if (newArraySize > entries.length) {
        long[] newEntries = new long[newArraySize];
        System.arraycopy(entries, 0, newEntries, 0, entries.length);
        entries = newEntries;
      }
      size = newSize;
    }
  }
  
  /**
   * Returns the boolean at position <code>i</code>
   * @param i
   * @return
   */
  public boolean get(long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    return (entries[entry] & (1L << offset)) != 0;
  }

  /**
   * Count number of set bits in the bit array.
   * Code adapted from
   * https://codingforspeed.com/a-faster-approach-to-count-set-bits-in-a-32-bit-integer/
   * @return
   */
  public long countOnes() {
    long totalCount = 0;
    for (long i : entries) {
      i = i - ((i >>> 1) & 0x5555555555555555L);
      i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
      i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
      i = i + (i >>> 8);
      i = i + (i >>> 16);
      i = i + (i >>> 32);
      totalCount += i & 0x7f;
    }
    return totalCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(size);
    int numEntriesToWrite = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    ByteBuffer bbuffer = ByteBuffer.allocate(numEntriesToWrite * 8);
    for (int i = 0; i < numEntriesToWrite; i++)
      bbuffer.putLong(entries[i]);
    // We should fill up the bbuffer
    assert bbuffer.remaining() == 0;
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position() - bbuffer.arrayOffset());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    size = in.readLong();
    int numEntriesToRead = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    if (entries == null || entries.length < numEntriesToRead)
      entries = new long[numEntriesToRead];
    byte[] buffer = new byte[numEntriesToRead * 8];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < numEntriesToRead; i++)
      entries[i] = bbuffer.getLong();
    assert !bbuffer.hasRemaining();
  }

  public long size() {
    return size;
  }

  public void fill(boolean b) {
    long fillValue = b ? 0xffffffffffffffffL : 0;
    for (int i = 0; i < entries.length; i++)
      entries[i] = fillValue;
  }

  public BitArray invert() {
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size();
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = ~this.entries[i];
    return result;
  }

  /**
   * Computes the bitwise OR of two bitmasks of the same size
   * @param other
   * @return
   */
  public BitArray or(BitArray other) {
    if (this.size != other.size)
      throw new RuntimeException("Cannot OR two BitArrays of different sizes");
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size;
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = this.entries[i] | other.entries[i];
    return result;
  }
}
