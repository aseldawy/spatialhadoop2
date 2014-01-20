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

package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Color;
import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * @author Ahmed Eldawy
 *
 */
public class NASARectangle extends Rectangle implements NASAShape {
  private static final byte[] Separator = {','};
  
  /**Value stored at this point*/
  public int value;

  public NASARectangle() {
  }

  public NASARectangle(Rectangle r) {
    super(r);
  }

  public NASARectangle(double x1, double y1, double x2, double y2) {
    this(x1, y1, x2, y2, 0);
  }

  public NASARectangle(double x1, double y1, double x2, double y2, int value) {
    super(x1, y1, x2, y2);
    this.value = value;
  }
  
  public int getValue() {
    return value;
  }
  
  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    value = in.readInt();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    text.append(Separator, 0, Separator.length);
    TextSerializerHelper.serializeInt(value, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    byte[] bytes = text.getBytes();
    text.set(bytes, 1, text.getLength() - 1);
    value = TextSerializerHelper.consumeInt(text, '\0');
  }
  
  @Override
  public String toString() {
    return super.toString() + " - "+value;
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, boolean vflip, double scale) {
    Color color;
    if (value < NASAPoint.minValue) {
      color = Color.getHSBColor(NASAPoint.MaxHue, 0.5f, 1.0f);
    } else if (value < NASAPoint.maxValue) {
      float ratio = NASAPoint.MaxHue - NASAPoint.MaxHue
          * (value - NASAPoint.minValue)
          / (NASAPoint.maxValue - NASAPoint.minValue);
      color = Color.getHSBColor(ratio, 0.5f, 1.0f);
    } else {
      color = Color.getHSBColor(0.0f, 0.5f, 1.0f);
    }
    g.setColor(color);
    super.draw(g, fileMBR, imageWidth, imageHeight, vflip, scale);
  }

}
