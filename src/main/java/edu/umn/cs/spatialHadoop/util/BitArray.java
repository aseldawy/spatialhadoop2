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
   * Returns the boolean at position <code>i</code>
   * @param i
   * @return
   */
  public boolean get(long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    return (entries[entry] & (1L << offset)) != 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(entries.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(entries.length * BitsPerEntry / 8);
    for (long entry : entries)
      bbuffer.putLong(entry);
    if (bbuffer.remaining() > 0)
      throw new RuntimeException("Error calculating the size of the buffer");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position() - bbuffer.arrayOffset());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    if (entries == null || entries.length != count)
      entries = new long[count];
    byte[] buffer = new byte[entries.length * BitsPerEntry / 8];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < entries.length; i++)
      entries[i] = bbuffer.getLong();
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not consume all entries");
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
    BitArray inverse = new BitArray();
    inverse.entries = new long[this.entries.length];
    for (int i = 0; i < entries.length; i++)
      inverse.entries[i] = ~this.entries[i];
    return inverse;
  }
}
