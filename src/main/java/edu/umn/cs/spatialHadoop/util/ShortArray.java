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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * Stores an expandable array of short integers
 * @author Ahmed Eldawy
 *
 */
public class ShortArray implements Writable, Iterable<Short> {
  /**Stores all elements*/
  protected short[] array;
  /**Number of entries occupied in array*/
  protected int size;
  
  public ShortArray() {
    this.array = new short[16];
  }
  
  public void add(short x) {
    append(x);
  }
  
  public void append(short x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(short[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(short[] xs, int offset, int count, short delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(ShortArray another) {
    append(another.array, 0, another.size);
  }
  
  public void append(ShortArray another, short delta) {
    append(another.array, 0, another.size, delta);
  }
  
  public boolean contains(short value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Ensures that the array can accept the additional entries
   * @param additionalSize
   */
  protected void expand(int additionalSize) {
    if (size + additionalSize > array.length) {
      int newCapacity = Math.max(size + additionalSize, array.length * 2);
      short[] newArray = new short[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  public static void writeShortArray(short[] array, DataOutput out) throws IOException {
    out.writeInt(array.length);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < array.length; i++) {
      bb.putShort(array[i]);
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
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < size; i++) {
      bb.putShort(array[i]);
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
  
  public static short[] readShortArray(short[] array, DataInput in) throws IOException {
    int newSize = in.readInt();
    // Check if we need to allocate a new array
    if (array == null || newSize != array.length)
      array = new short[newSize];
    byte[] buffer = new byte[1024*1024];
    int size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 2));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getShort();
    }
    return array;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int newSize = in.readInt();
    expand(newSize);
    byte[] buffer = new byte[1024*1024];
    size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 2));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getShort();
    }
  }
  
  public int size() {
    return size;
  }
  
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the underlying array. The returned array might have a length that
   * is larger than {@link #size()}. The values of those additional slots are
   * undefined and should not be used.
   * @return
   */
  public short[] underlyingArray() {
    return array;
  }
  
  /**
   * Converts this ShortArray into a native Java array that with a length equal
   * to {@link #size()}.
   * @return
   */
  public short[] toArray() {
    short[] compactArray = new short[size];
    System.arraycopy(array, 0, compactArray, 0, size);
    return compactArray;
  }
  
  public void sort() {
    Arrays.sort(array, 0, size);
  }
  
  /**
   * Searches the underlying array using the {@link Arrays#binarySearch(short[], short)}
   * function.
   * @param key The value to find
   * @return The position of the given value as returned by {@link Arrays#binarySearch(short[], short)}
   */
  public int binarySearch(short key) {
    return Arrays.binarySearch(array, 0, size, key);
  }

  public short get(int index) {
    return array[index];
  }
  
  public int pop() {
    return array[--size];
  }
  
  public boolean remove(short value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        System.arraycopy(array, i + 1, array, i, size - (i + 1));
        size--;
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<Short> iterator() {
    return new ShortIterator();
  }
  
  class ShortIterator implements Iterator<Short> {
    int i = -1;

    @Override
    public boolean hasNext() {
      return i < size() - 1;
    }

    @Override
    public Short next() {
      return array[++i];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not yet supported");
    }
    
  }

  public void swap(int i, int j) {
    short t = array[i];
    array[i] = array[j];
    array[j] = t;
  }
  
  @Override
  public String toString() {
    int iMax = size - 1;
    if (iMax == -1)
      return "[]";

    StringBuilder b = new StringBuilder();
    b.append('[');
    for (int i = 0; ; i++) {
      b.append(array[i]);
      if (i == iMax)
        return b.append(']').toString();
      b.append(", ");
    }
  }
}
