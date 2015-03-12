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
 * Data descriptor for array of scientific data.
 * @author Eldawy
 *
 */
public class DDScientificData extends DataDescriptor {

  /**Raw data*/
  public byte[] data;
  
  DDScientificData(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    data = new byte[getLength()];
    input.readFully(data);
  }

  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Scientific data of size %d", getLength());
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }

  public byte[] getData() throws IOException {
    lazyLoad();
    return data;
  }
}
