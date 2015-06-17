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
 * A data descriptor for linked block table
 * @author Ahmed Eldawy
 *
 */
public class DDLinkedBlock extends DataDescriptor {

  /**
   * @param hdfFile
   * @param tagID
   * @param refNo
   * @param offset
   * @param length
   * @param extended
   */
  public DDLinkedBlock(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    throw new RuntimeException("Non-implemented method");
  }

}
