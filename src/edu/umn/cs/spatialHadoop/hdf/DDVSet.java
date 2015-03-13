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
 */
public class DDVSet extends DataDescriptor {

  /**
   * The data stored in this set. This data can be interpreted only
   * according to the corresponding {@link DDVDataHeader}
   */
  protected byte[] data;

  DDVSet(HDFFile hdfFile, int tagID, int refNo, int offset, int length,
      boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    this.data = new byte[getLength()];
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
      return String.format("VSet of total size %d", data.length);
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }
}
