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
 * A place holder for data descriptors with unsupported tag number
 * @author Ahmed Eldawy
 *
 */
public class DDUnknown extends DataDescriptor {

  public byte[] rawData;
  
  DDUnknown(HDFFile hdfFile, int tagID, int refNo, int offset, int length,
      boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }
  
  @Override
  protected void readFields(DataInput input) throws IOException {
  }

  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Unknown tag %d with data of size %d", tagID, getLength());
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }
}
