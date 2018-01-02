/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A shape from tiger file.
 * @author Ahmed Eldawy
 *
 */
public class TigerShape extends OGCJTSShape {
  /** Full text line of the input */
  private String originalText;
  
  @Override
  public void fromText(Text text) {
    originalText = text.toString();
    super.fromText(text);
  }
  
  @Override
  public Text toText(Text text) {
    text.append(originalText.getBytes(), 0, originalText.length());
    return text;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = originalText.getBytes();
    out.writeInt(bytes.length);
    out.write(bytes);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    this.originalText = new String(bytes);
    this.fromText(new Text(originalText));
  }
  
  @Override
  public Shape clone() {
    TigerShape c = new TigerShape();
    c.originalText = this.originalText;
    c.geom = this.geom;
    return c;
  }

  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    super.draw(g, xscale, yscale);
    // Draw label if possible
    // A label is possible to draw if its MBR is smaller than the MBR of the shape
    // For simplicity, we just calculate the width and consider it possible if
    // it the width is smaller than the width of the shape

    // 1- Calculate string width
    String label = originalText.split("\t")[2];
    //String label = originalText.split("\t")[1];
    int strWidth = g.getFontMetrics().stringWidth(label);

    // 2- Calculate the shape width
    Rectangle mbr = getMBR();
    int mbrWidth = (int) Math.round(mbr.getWidth() * xscale);

    if (strWidth < mbrWidth) {
      g.drawString(label,
          (int) Math.round(mbr.x1 * xscale - (mbrWidth - strWidth)),
          (int) Math.round(mbr.y1 * yscale - g.getFontMetrics().getHeight() / 2));
    }
  }
}
