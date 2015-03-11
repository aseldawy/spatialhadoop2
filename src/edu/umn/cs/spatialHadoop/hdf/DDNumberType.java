/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * @author Ahmed Eldawy
 *
 */
public class DDNumberType extends DataDescriptor {

  /**Version number of th NT information*/
  protected int version;
  
  /**
   * Type of the data:
   * Unsigned integer, signed integer, unsigned character, character,
   * floating point, double precision floating point
   */
  protected int type;

  /**Number of bits, all of which are assumed to be significant*/
  protected int width;

  /**
   * A generic value, with different interpretations depending on type:
   * flaoting point, integer, or character
   */
  protected int klass;

  public DDNumberType() {
  }
  
  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    this.version = in.readUnsignedByte();
    this.type = in.readUnsignedByte();
    this.width = in.readUnsignedByte();
    this.klass = in.readUnsignedByte();
  }

  @Override
  public String toString() {
    return String.format("Number type %d %d %d %d", version, type, width, klass);
  }
}
