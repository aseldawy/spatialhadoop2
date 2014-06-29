/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
