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
import edu.umn.cs.spatialHadoop.nasa.NASAPoint.GradientType;

/**
 * @author Ahmed Eldawy
 *
 */
public class NASARectangle extends Rectangle implements NASAShape {
  private static final byte[] Separator = {','};
  
  /**Value stored at this point*/
  public int value;
  
  public long timestamp;

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
    out.writeLong(timestamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    value = in.readInt();
    this.timestamp = in.readLong();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    text.append(Separator, 0, Separator.length);
    TextSerializerHelper.serializeInt(value, text, ',');
    TextSerializerHelper.serializeLong(timestamp, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    byte[] bytes = text.getBytes();
    text.set(bytes, 1, text.getLength() - 1);
    value = TextSerializerHelper.consumeInt(text, ',');
    timestamp = TextSerializerHelper.consumeLong(text, '\0');
  }
  
  @Override
  public String toString() {
    return super.toString() + " - "+value;
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    g.setColor(calculateColor(this.value));
    super.draw(g, fileMBR, imageWidth, imageHeight, scale);
  
    char[] textTemperature=(this.value+"").toCharArray();
    g.setColor(Color.black);
    
    int s_x1 = (int) Math.round((this.x1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y1 = (int) Math.round((this.y1 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    
    g.drawChars(textTemperature, 0, textTemperature.length,(int)(s_x1),(int) (s_y1));

  }

  public static Color calculateColor(int value) {
    Color color;
    if (value < NASAPoint.minValue) {
      color = NASAPoint.color1;
    } else if (value > NASAPoint.maxValue) {
      color = NASAPoint.color2;
    } else {
      // Interpolate between two colors according to gradient type
      float ratio = (value - NASAPoint.minValue) / (NASAPoint.maxValue - NASAPoint.minValue);
      if (NASAPoint.gradientType == GradientType.GT_HUE) {
        // Interpolate between two hues
        float hue = NASAPoint.hue1 * (1.0f - ratio) + NASAPoint.hue2 * ratio;
        color = Color.getHSBColor(hue, 0.5f, 1.0f);
      } else if (NASAPoint.gradientType == GradientType.GT_COLOR) {
        // Interpolate between colors
        int red = (int) (NASAPoint.color1.getRed() * (1.0f - ratio) + NASAPoint.color2.getRed() * ratio);
        int green = (int) (NASAPoint.color1.getGreen() * (1.0f - ratio) + NASAPoint.color2.getGreen() * ratio);
        int blue = (int) (NASAPoint.color1.getBlue() * (1.0f - ratio) + NASAPoint.color2.getBlue() * ratio);
        int alpha = (int) (NASAPoint.color1.getAlpha() * (1.0f - ratio) + NASAPoint.color2.getAlpha() * ratio);
        color = new Color(red, green, blue, alpha);
      } else {
        throw new RuntimeException("Unsupported gradient type: "+NASAPoint.gradientType);
      }
    }
    return color;
  }
  
  @Override
  public void setTimestamp(long t) {
    this.timestamp = t;
  }

  @Override
  public long getTimestamp() {
    return this.timestamp;
  }
}
