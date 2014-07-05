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
package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class Partition extends CellInfo {
  /**Name of the file that contains the data*/
  public String filename;
  
  /**Total number of records in this partition*/
  public long recordCount;
  
  /**Total size of data in this partition in bytes (uncompressed)*/
  public long size;
  
  public Partition() {}
  
  public Partition(String filename, CellInfo cell) {
    this.filename = filename;
    super.set(cell);
  }
  
  public Partition(Partition other) {
    this.filename = other.filename;
    this.recordCount = other.recordCount;
    this.size = other.size;
    super.set((CellInfo)other);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(filename);
    out.writeLong(recordCount);
    out.writeLong(size);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    filename = in.readUTF();
    this.recordCount = in.readLong();
    this.size = in.readLong();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    text.append(new byte[] {','}, 0, 1);
    TextSerializerHelper.serializeLong(recordCount, text, ',');
    TextSerializerHelper.serializeLong(size, text, ',');
    byte[] temp = (filename == null? "" : filename).getBytes();
    text.append(temp, 0, temp.length);
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    text.set(text.getBytes(), 1, text.getLength() - 1); // Skip comma
    this.recordCount = TextSerializerHelper.consumeLong(text, ',');
    this.size = TextSerializerHelper.consumeLong(text, ',');
    filename = text.toString();
  }
  
  @Override
  public Partition clone() {
    return new Partition(this);
  }
  
  @Override
  public int hashCode() {
    return filename.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return this.filename.equals(((Partition)obj).filename);
  }
  
  public void expand(Partition p) {
    super.expand(p);
    // accumulate size
    this.size += p.size;
    this.recordCount += p.recordCount;
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    int s_x1 = (int) Math.round((this.x1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y1 = (int) Math.round((this.y1 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    int s_x2 = (int) Math.round((this.x2 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y2 = (int) Math.round((this.y2 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
  }
}