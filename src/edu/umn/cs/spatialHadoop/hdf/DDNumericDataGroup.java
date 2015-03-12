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
import java.util.Arrays;

/**
 * Scientific data set tags
 * @author Ahmed Eldawy
 *
 */
public class DDNumericDataGroup extends DataDescriptor {

  /**Mumbers of the group*/
  protected DDID[] members;

  DDNumericDataGroup(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }
  
  @Override
  protected void readFields(DataInput input) throws IOException {
    int numOfDataObjects = getLength() / 4;
    members = new DDID[numOfDataObjects];
    for (int i = 0; i < numOfDataObjects; i++) {
      int tagID = input.readUnsignedShort();
      int refNo = input.readUnsignedShort();
      members[i] = new DDID(tagID, refNo);
    }
  }
  
  public DataDescriptor[] getContents() throws IOException {
    lazyLoad();
    DataDescriptor[] contents = new DataDescriptor[members.length];
    for (int i = 0; i < contents.length; i++)
      contents[i] = hdfFile.retrieveElementByID(members[i]);
    return contents;
  }
  
  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Numeric data groups %s", Arrays.toString(members));
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }
}
