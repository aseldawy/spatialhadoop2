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
  }
  
  public void draw(Graphics g, double xscale, double yscale) {
    g.setColor(calculateColor(this.value));
    super.draw(g, xscale, yscale);
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
        float saturation = NASAPoint.saturation1 * (1.0f - ratio) + NASAPoint.saturation2 * ratio;
        float brightness = NASAPoint.brightness1 * (1.0f - ratio) + NASAPoint.brightness2 * ratio;
        color = Color.getHSBColor(hue, saturation, brightness);
        int alpha = (int) (NASAPoint.color1.getAlpha() * (1.0f - ratio) + NASAPoint.color2.getAlpha() * ratio);
        color = new Color(color.getRGB() & 0xffffff | (alpha << 24), true);
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
  
  public static Color calculateColor(float value) {
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
	        float saturation = NASAPoint.saturation1 * (1.0f - ratio) + NASAPoint.saturation2 * ratio;
	        float brightness = NASAPoint.brightness1 * (1.0f - ratio) + NASAPoint.brightness2 * ratio;
	        color = Color.getHSBColor(hue, saturation, brightness);
	        int alpha = (int) (NASAPoint.color1.getAlpha() * (1.0f - ratio) + NASAPoint.color2.getAlpha() * ratio);
	        color = new Color(color.getRGB() & 0xffffff | (alpha << 24), true);
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
  
  @Override
  public Rectangle clone() {
    NASARectangle c = new NASARectangle(this);
    c.value = this.value;
    c.timestamp = this.timestamp;
    return c;
  }
}
