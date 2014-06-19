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
package edu.umn.cs.spatialHadoop;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Writable;

/**
 * An in-memory image that can be used with MapReduce
 * @author eldawy
 *
 */
public class ImageWritable implements Writable {
  /**
   * The underlying image
   */
  private BufferedImage image;
  
  /**
   * A default constructor is necessary to be readable/writable
   */
  public ImageWritable() {
  }

  public ImageWritable(BufferedImage image) {
    this.image = image;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageIO.write(this.image, "png", baos);
    baos.close();
    byte[] bytes = baos.toByteArray();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    this.image = ImageIO.read(new ByteArrayInputStream(bytes));
  }

  public BufferedImage getImage() {
    return image;
  }
  
  public void setImage(BufferedImage image) {
    this.image = image;
  }
}
