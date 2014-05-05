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

public class Partition extends CellInfo {
  /**Name of the file that contains the data*/
  public String filename;
  
  public Partition() {}
  
  public Partition(String filename, CellInfo cell) {
    this.filename = filename;
    this.set(cell);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(filename);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    filename = in.readUTF();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    byte[] temp = (","+filename).getBytes();
    text.append(temp, 0, temp.length);
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    // Skip the comma and read filename
    filename = new String(text.getBytes(), 1, text.getLength() - 1);
  }
  
  @Override
  public Partition clone() {
    return new Partition(filename, this);
  }
  
  @Override
  public int hashCode() {
    return filename.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return this.filename.equals(((Partition)obj).filename);
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, boolean vflip, double scale) {
    int s_x1 = (int) Math.round((this.x1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y1 = (int) Math.round(((vflip? -this.y2 : this.y1) - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    int s_x2 = (int) Math.round((this.x2 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y2 = (int) Math.round(((vflip? -this.y1 : this.y2) - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
  }
}