/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Color;
import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class NASAPoint extends Point implements NASAShape {
  
  private static final byte[] Separator = {','};
  
  /**Value stored at this point*/
  public int value;
  
  /** Timestamp of this point as appear in the HDF file*/
  public long timestamp;
  
  public NASAPoint() {}
  
  public NASAPoint(double x, double y, int value, long timestamp) {
    super(x, y);
    this.value = value;
    this.timestamp = timestamp;
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
    this.value = in.readInt();
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
  
  /**Valid range of values. Used for drawing.*/
  public static float minValue, maxValue;
  
  protected static Color color1, color2;
  protected static float hue1, hue2;
  protected static float saturation1, saturation2;
  protected static float brightness1, brightness2;
  
  public static enum GradientType {GT_HUE, GT_COLOR};
  public static GradientType gradientType;

  public static void setColor1(Color color) {
    color1 = color;
    float[] hsbvals = new float[3];
    Color.RGBtoHSB(color.getRed(), color.getGreen(), color.getBlue(), hsbvals);
    hue1 = hsbvals[0];
    saturation1 = hsbvals[1];
    brightness1 = hsbvals[2];
  }
  
  public static void setColor2(Color color) {
    color2 = color;
    float[] hsbvals = new float[3];
    Color.RGBtoHSB(color.getRed(), color.getGreen(), color.getBlue(), hsbvals);
    hue2 = hsbvals[0];
    saturation2 = hsbvals[1];
    brightness2 = hsbvals[2];
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    g.setColor(NASARectangle.calculateColor(this.value));
    super.draw(g, fileMBR, imageWidth, imageHeight, scale);
  }

  @Override
  public void setTimestamp(long t) {
    this.timestamp = t;
  }

  @Override
  public long getTimestamp() {
    return this.timestamp;
  }
  
  @Override
  public NASAPoint clone() {
    return new NASAPoint(x, y, value, timestamp);
  }
}
