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
 * Stores an expandable array of integers
 * @author Ahmed Eldawy
 *
 */
public class IntArray implements Writable {
  /**Stores all elements*/
  protected int[] array;
  /**Number of entries occupied in array*/
  protected int size;
  
  public IntArray() {
    this.array = new int[16];
  }
  
  public void append(int x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(int[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(int[] xs, int offset, int count, int delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(IntArray another) {
    append(another.array, 0, another.size);
  }
  
  public void append(IntArray another, int delta) {
    append(another.array, 0, another.size, delta);
  }
  
  /**
   * Ensures that the array can accept the additional entries
   * @param additionalSize
   */
  protected void expand(int additionalSize) {
    if (size + additionalSize > array.length) {
      int newCapacity = Math.max(size + additionalSize, array.length * 2);
      int[] newArray = new int[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < size; i++) {
      bb.putInt(array[i]);
      if (bb.position() == bb.capacity()) {
        // Full. Write to output
        out.write(bb.array(), 0, bb.position());
        bb.clear();
      }
    }
    // Write whatever remaining in the buffer
    out.write(bb.array(), 0, bb.position());
    bb.clear();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int newSize = in.readInt();
    expand(newSize);
    byte[] buffer = new byte[1024*1024];
    size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 4));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getInt();
    }
  }
  
  public int size() {
    return size;
  }
  
  public int[] array() {
    return array;
  }
  
  public int get(int index) {
    return array[index];
  }
}
