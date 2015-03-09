/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A shape from tiger file.
 * @author aseldawy
 *
 */
public class TigerShape extends OGCJTSShape {
  private String originalText;
  
  @Override
  public void fromText(Text text) {
    originalText = text.toString();
    byte[] bytes = text.getBytes();
    int i = 0;
    while (i < text.getLength() && bytes[i] != ',') {
      i++;
    }
    text.set(bytes, 0, i);
    super.fromText(text);
  }
  
  @Override
  public Text toText(Text text) {
    text.set(originalText);
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
}
