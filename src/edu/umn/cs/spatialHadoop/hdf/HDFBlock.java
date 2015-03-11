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
 * A Data Description Block in an HDF file
 * @author Eldawy
 *
 */
public class HDFBlock {

  public DataDescriptor[] dataDescriptors;
  
  /**
   * 
   */
  public HDFBlock() {
  }

  /**
   * Reads block information from the HDF file.
   * @param in - data input from the HDF file
   * @param numDD - number of data descriptors in this block
   * @throws IOException 
   */
  public void readFields(FSDataInputStream in, int numDD) throws IOException {
    dataDescriptors = new DataDescriptor[numDD];
    for (int iDD = 0; iDD < numDD; iDD++) {
      int tagID = in.readUnsignedShort();
      if (tagID == HDFConstants.DFTAG_VERSION) {
        // Version
        dataDescriptors[iDD] = new DDVersion();
      } else if (tagID == HDFConstants.DFTAG_NULL || tagID == 0) {
        // No data
        dataDescriptors[iDD] = new DDNull();
      } else if (tagID == HDFConstants.DFTAG_SD || tagID == HDFConstants.DFTAG_SD_E) {
        // Scientific data
        dataDescriptors[iDD] = new DDScientificData();
      } else if (tagID == HDFConstants.DFTAG_SDD) {
        // Scientific data dimension record
        dataDescriptors[iDD] = new DDScientificDDR();
      } else if (tagID == HDFConstants.DFTAG_NT) {
        // Number type
        dataDescriptors[iDD] = new DDNumberType();
      } else if (tagID == HDFConstants.DFTAG_COMPRESSED) {
        // Compressed data
        dataDescriptors[iDD] = new DDCompressedBlock();
      } else if (tagID == HDFConstants.DFTAG_VH) {
        // Vdata header
        dataDescriptors[iDD] = new DDVDataHeader();
      } else if (tagID == HDFConstants.DFTAG_VS) {
        // VSet
        dataDescriptors[iDD] = new DDVSet();
      } else if (tagID == HDFConstants.DFTAG_VG) {
        // VGroup
        dataDescriptors[iDD] = new DDVGroup();
      } else if (tagID == HDFConstants.DFTAG_NDG) {
        // Numeric data group
        dataDescriptors[iDD] = new DDNumericDataGroup();
      } else {
        dataDescriptors[iDD] = new DDUnknown();
      }
      dataDescriptors[iDD].tagID = tagID;
      dataDescriptors[iDD].refNo = in.readUnsignedShort();
      dataDescriptors[iDD].offset = in.readInt();
      dataDescriptors[iDD].length = in.readInt();
    }
    for (int iDD = 0; iDD < numDD; iDD++) {
      dataDescriptors[iDD].readFields(in);
      System.out.println(dataDescriptors[iDD]);
    }
  }
}
