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
 */
public class DDVSet extends DataDescriptor {

  /**
   * The raw data stored in this set. This data can be interpreted only
   * according to the corresponding {@link DDVDataHeader}
   */
  protected byte[] rawData;

  public DDVSet() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    this.rawData = readRawData(in);
  }
  
  @Override
  public String toString() {
    return String.format("VSet of total size %d", rawData.length);
  }
}
