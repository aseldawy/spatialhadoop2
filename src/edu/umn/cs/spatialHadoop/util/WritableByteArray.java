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

import org.apache.hadoop.io.Writable;

/**
 * @author Ahmed Eldawy
 *
 */
public class WritableByteArray implements Writable {
  /**The buffer that holds the data*/
  private byte[] buffer;
  /**Number of correct bytes in the buffer*/
  private int length;

  public WritableByteArray() {
  }
  
  public WritableByteArray(byte[] b) {
    this.buffer = b;
    this.length = b.length;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    out.write(buffer, 0, length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.length = in.readInt();
    if (this.buffer.length < this.length)
      this.buffer = new byte[length];
    in.readFully(buffer, 0, this.length);
  }

  public void set(byte[] b, int start, int end) {
    if (buffer.length < end - start)
      buffer = new byte[(end - start) * 2];
    if (b != buffer || start != 0)
      System.arraycopy(b, start, buffer, 0, end - start);
    this.length = end - start;
  }

  public int getLength() {
    return length;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void write(byte[] b, int start, int end) {
    this.set(b, start, end);
  }

  public byte[] getData() {
    return buffer;
  }

}
