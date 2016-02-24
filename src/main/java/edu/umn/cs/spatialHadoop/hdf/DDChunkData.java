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
 * A data descriptor for chunk data.
 * Tag DFTAG_CHUNK
 * @author Ahmed Eldawy
 *
 */
public class DDChunkData extends DataDescriptor {
  
  /**The data in this chunk*/
  protected byte[] data;

  DDChunkData(HDFFile hdfFile, int tagID, int refNo, int offset, int length,
      boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    data = new byte[getLength()];
    input.readFully(data);
  }
  
  byte[] getData() throws IOException {
    lazyLoad();
    return data;
  }
  
  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Chunk data of length %d", getLength());
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }

}
