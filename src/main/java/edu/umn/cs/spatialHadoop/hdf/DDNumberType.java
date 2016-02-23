/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.DataInput;
import java.io.IOException;

/**
 * @author Ahmed Eldawy
 *
 */
public class DDNumberType extends DataDescriptor {

  /**Version number of the number type information*/
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
   * floating point, integer, or character.
   */
  protected int klass;

  DDNumberType(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }
  
  @Override
  protected void readFields(DataInput input) throws IOException {
    this.version = input.readUnsignedByte();
    this.type = input.readUnsignedByte();
    this.width = input.readUnsignedByte();
    this.klass = input.readUnsignedByte();
  }

  public int getNumberType() throws IOException {
    lazyLoad();
    return type;
  }

  public int getDataSize() throws IOException {
    lazyLoad();
    switch (type) {
    case HDFConstants.DFNT_UINT8: return 1;
    case HDFConstants.DFNT_INT16:
    case HDFConstants.DFNT_UINT16: return 2;
    case HDFConstants.DFNT_INT32: return 4;
    default: throw new RuntimeException("Unsupported type "+type);
    }
  }

  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Number type %d %d %d %d", version, type, width, klass);
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }

}
