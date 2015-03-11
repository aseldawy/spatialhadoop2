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
 * Scientific data set tags
 * @author Ahmed Eldawy
 *
 */
public class DDNumericDataGroup extends DataDescriptor {

  /**Tag number of the members of the group*/
  protected int[] tags;
  /**Reference number of the members of the group*/
  protected int[] refs;

  public DDNumericDataGroup() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    int numOfDataObjects = this.length / 4;
    this.tags = new int[numOfDataObjects];
    this.refs = new int[numOfDataObjects];
    for (int i = 0; i < numOfDataObjects; i++) {
      this.tags[i] = in.readUnsignedShort();
      this.refs[i] = in.readUnsignedShort();
    }
  }
  
  @Override
  public String toString() {
    return String.format("Numeric data groups %d", this.tags.length);
  }
}
