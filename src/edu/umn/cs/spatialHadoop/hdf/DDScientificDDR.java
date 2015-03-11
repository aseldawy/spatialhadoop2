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
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Scientific data dimension record.
 * @author Ahmed Eldawy
 *
 */
public class DDScientificDDR extends DataDescriptor {

  /**Number of values along each dimension*/
  protected int[] dimensions;
  protected int data_NT_ref;
  protected int[] scale_NT_refs;

  public DDScientificDDR() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    // Number of dimensions
    int rank = in.readUnsignedShort();
    this.dimensions = new int[rank];
    for (int iDim = 0; iDim < rank; iDim++)
      this.dimensions[iDim] = in.readInt();
    int marker = in.readUnsignedShort();
    if (marker != HDFConstants.DFTAG_NT)
      throw new RuntimeException("Did not find DFTAG_NT but "+marker);
    this.data_NT_ref = in.readUnsignedShort();
    this.scale_NT_refs = new int[rank];
    for (int iDim = 0; iDim < rank; iDim++) {
      marker = in.readUnsignedShort();
      if (marker != HDFConstants.DFTAG_NT)
        throw new RuntimeException("Did not find DFTAG_NT but "+marker);
      this.scale_NT_refs[iDim] = in.readUnsignedShort();
    }
  }
  
  @Override
  public String toString() {
    return String.format("Scientific data with dimensions %s", Arrays.toString(dimensions));
  }
}
