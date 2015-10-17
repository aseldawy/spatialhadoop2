/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * Stores an expandable array of primitive floats
 * @author Ahmed Eldawy
 *
 */
public class FloatArray implements Writable, Iterable<Float> {
  /**Stores all elements*/
  protected float[] array;
  /**Number of entries occupied in array*/
  protected int size;
  
  public FloatArray() {
    this.array = new float[16];
  }
  
  public void add(float x) {
    append(x);
  }
  
  public void append(float x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(float[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(float[] xs, int offset, int count, float delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(FloatArray another) {
    append(another.array, 0, another.size);
  }
  
  public void append(FloatArray another, float delta) {
    append(another.array, 0, another.size, delta);
  }
  
  public boolean contains(float value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value)
        return true;
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
      float[] newArray = new float[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  public static void writeFloatArray(float[] array, DataOutput out) throws IOException {
    out.writeInt(array.length);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < array.length; i++) {
      bb.putFloat(array[i]);
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
      bb.putFloat(array[i]);
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
  
  public static float[] readFloatArray(float[] array, DataInput in) throws IOException {
    int newSize = in.readInt();
    // Check if we need to allocate a new array
    if (array == null || newSize != array.length)
      array = new float[newSize];
    byte[] buffer = new byte[1024*1024];
    int size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 4));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getFloat();
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
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 4));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getFloat();
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
  public float[] underlyingArray() {
    return array;
  }
  
  /**
   * Converts this IntArray into a native Java array that with a length equal
   * to {@link #size()}.
   * @return
   */
  public float[] toArray() {
    float[] compactArray = new float[size];
    System.arraycopy(array, 0, compactArray, 0, size);
    return compactArray;
  }
  
  public void sort() {
    Arrays.sort(array, 0, size);
  }

  public float get(int index) {
    return array[index];
  }
  
  public float pop() {
    return array[--size];
  }
  
  public boolean remove(float value) {
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
  public Iterator<Float> iterator() {
    return new FloatIterator();
  }
  
  class FloatIterator implements Iterator<Float> {
    int i = -1;

    @Override
    public boolean hasNext() {
      return i < size() - 1;
    }

    @Override
    public Float next() {
      return array[++i];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not yet supported");
    }
    
  }

  public void swap(int i, int j) {
    float t = array[i];
    array[i] = array[j];
    array[j] = t;
  }
}
