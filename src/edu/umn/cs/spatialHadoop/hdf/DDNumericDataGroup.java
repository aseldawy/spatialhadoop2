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
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Scientific data set tags
 * @author Ahmed Eldawy
 *
 */
public class DDNumericDataGroup extends DataDescriptor {

  /**Members of the group*/
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

  /**
   * Return the underlying data without doing any reformatting.
   * @return
   * @throws IOException
   */
  public byte[] getAsByteArray() throws IOException {
    lazyLoad();
    for (int i = 0; i < members.length; i++)
      if (members[i].tagID == HDFConstants.DFTAG_SD)
        return ((DDScientificData)hdfFile.retrieveElementByID(members[i])).getData();
    return null;
  }

  public void getAsByteArray(byte[] buf, int bufOff, int bufLen) throws IOException {
    lazyLoad();
    for (int i = 0; i < members.length; i++)
      if (members[i].tagID == HDFConstants.DFTAG_SD) {
        ((DDScientificData)hdfFile.retrieveElementByID(members[i])).readData(buf, bufOff, bufLen);
        return;
      }
  }
  
  /**
   * Returns the type of the underlying data
   * @return
   * @throws IOException
   */
  public int getDataType() throws IOException {
    lazyLoad();
    for (int i = 0; i < members.length; i++) {
      if (members[i].tagID == HDFConstants.DFTAG_NT) {
        // Number type
        DDNumberType nt = (DDNumberType)hdfFile.retrieveElementByID(members[i]);
        return nt.getNumberType();
      }
    }
    return -1;
  }
  
  public int getDataSize() throws IOException {
    lazyLoad();
    for (int i = 0; i < members.length; i++) {
      if (members[i].tagID == HDFConstants.DFTAG_NT) {
        // Number type
        DDNumberType nt = (DDNumberType)hdfFile.retrieveElementByID(members[i]);
        return nt.getDataSize();
      }
    }
    return 0;
  }

  
  public Object getAsTypedArray() throws IOException {
    lazyLoad();
    int overallSize = 0;
    int type = 0;
    byte[] rawData = null;
    for (int i = 0; i < members.length; i++) {
      if (members[i].tagID == HDFConstants.DFTAG_SDD) {
        // Dimensions of the array
        DDScientificDDR ddr = (DDScientificDDR)hdfFile.retrieveElementByID(members[i]);
        overallSize = 1;
        for (int dim : ddr.getDimensions())
          overallSize *= dim;
      } else if (members[i].tagID == HDFConstants.DFTAG_NT) {
        // Number type
        DDNumberType nt = (DDNumberType)hdfFile.retrieveElementByID(members[i]);
        type = nt.getNumberType();
      } else if (members[i].tagID == HDFConstants.DFTAG_SD) {
        rawData = ((DDScientificData)hdfFile.retrieveElementByID(members[i])).getData();
      }
    }
    if (rawData == null) {
      return null;
    }
    ByteBuffer bbuffer = ByteBuffer.wrap(rawData);
    switch (type) {
    case HDFConstants.DFNT_UINT16:
      short[] values = new short[overallSize];
      for (int i = 0; i < values.length; i++)
        values[i] = bbuffer.getShort();
      return values;
    case HDFConstants.DFNT_UINT8:
      return rawData;
    default:
      throw new RuntimeException("Unsupported type "+type);
    }
  }
  
  public int[] getDimensions() throws IOException {
    for (int i = 0; i < members.length; i++) {
      if (members[i].tagID == HDFConstants.DFTAG_SDD) {
        // Dimensions of the array
        DDScientificDDR ddr = (DDScientificDDR)hdfFile.retrieveElementByID(members[i]);
        return ddr.getDimensions();
      }
    }
    return null;
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
