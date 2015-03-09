/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Loads a shape from a specific column in a CSV file. Both the separator and
 * the column to load are configurable.
 * 
 * @author Ahmed Eldawy
 */
public class CSVOGC extends OGCJTSShape {

  /**Index of the column that contains the shape*/
  private byte column = 0;
  
  /**Column separator. Use tab as the default to match with PigStorage*/
  private char separator = '\t';
  
  /**All data in the line before the shape*/
  private byte[] prefix;
  
  /**All data in the line after the shape*/
  private byte[] suffix;
  
  /**
   * 
   */
  public CSVOGC() {
  }

  public void setColumn(int column) {
    this.column = (byte) column;
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  /**
   * @param geom
   */
  public CSVOGC(Geometry geom) {
    super(geom);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      prefix = new byte[size];
      in.readFully(prefix);
    } else {
      prefix = null;
    }
    super.readFields(in);
    size = in.readInt();
    if (size > 0) {
      suffix = new byte[size];
      in.readFully(suffix);
    } else {
      suffix = null;
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    if (prefix == null) {
      out.writeInt(0);
    } else {
      out.writeInt(prefix.length);
      out.write(prefix);
    }
    super.write(out);
    if (suffix == null) {
      out.writeInt(0);
    } else {
      out.writeInt(suffix.length);
      out.write(suffix);
    }
  }
  
  @Override
  public Text toText(Text text) {
    if (prefix != null)
      text.append(prefix, 0, prefix.length);
    super.toText(text);
    if (suffix != null)
      text.append(suffix, 0, suffix.length);
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    byte[] bytes = text.getBytes();
    int separatorsEncountered = 0;
    int i1 = 0;
    // Locate the required column
    while (separatorsEncountered < column && i1 < text.getLength()) {
      if (bytes[i1++] == separator)
        separatorsEncountered++;
    }
    if (i1 == text.getLength()) {
      this.prefix = new byte[i1];
      System.arraycopy(bytes, 0, prefix, 0, i1);
      super.geom = null;
      this.suffix = null;
      return;
    }
    int i2 = i1+1;
    while (i2 < text.getLength() && bytes[i2] != separator)
      i2++;
    // Copy prefix and suffix
    if (i1 == 0) {
      prefix = null;
    } else {
      prefix = new byte[i1];
      System.arraycopy(bytes, 0, prefix, 0, i1);
    }
    if (i2 == text.getLength()) {
      suffix = null;
    } else {
      suffix = new byte[text.getLength() - i2];
      System.arraycopy(bytes, i2, suffix, 0, text.getLength() - i2);
    }
    
    // Chop prefix and suffix and leave only the selected column
    text.set(bytes, i1, i2 - i1);
    super.fromText(text);
  }
  
  @Override
  public String toString() {
    return (prefix == null? "" : new String(prefix)) + super.toString() + (suffix == null? "" : new String(suffix));
  }
  
  @Override
  public Shape clone() {
    CSVOGC c = new CSVOGC();
    c.separator = this.separator;
    c.column = this.column;
    return c;
  }
}
