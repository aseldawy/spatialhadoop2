/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Parses and HDF4 file
 * @author Ahmed Eldawy
 *
 */
public class HDFFile implements Closeable {
  
  /**Special marker for HDF files*/
  private static final byte[] HDFMagicNumber = {0x0E, 0x03, 0x13, 0x01};
  
  /**A set of all data descriptors indexed by their unique IDs*/
  protected Map<DDID, DataDescriptor> dataDescriptors =
      new HashMap<DDID, DataDescriptor>();
  
  /**Input stream to the underlying */
  FSDataInputStream inStream;
  
  /**
   * Initializes a new HDF file from an input stream. This stream should not
   * be closed as long as the HDF file is used. Closing this HDFFile will
   * also close the underlying stream.
   * @throws IOException 
   * 
   */
  public HDFFile(FSDataInputStream inStream) throws IOException {
    this.inStream = inStream;
    byte[] signature = new byte[4];
    inStream.readFully(signature);
    if (!Arrays.equals(signature, HDFMagicNumber))
      throw new RuntimeException("Not a valid HDF file");
    int blockSize = inStream.readUnsignedShort();
    int nextBlock;
    do {
      // Keep track of the location of the next block
      nextBlock = inStream.readInt();
      this.readBlock(blockSize);
      if (nextBlock != 0) {
        inStream.seek(nextBlock);
        blockSize = inStream.readShort();
      }
    } while (nextBlock > 0);
  }

  /**
   * Reads block information from the HDF file.
   * @param numDD number of data descriptors in this block
   * @throws IOException
   */
  public void readBlock(int numDD) throws IOException {
    for (int iDD = 0; iDD < numDD; iDD++) {
      DataDescriptor dd;
      int tagID = inStream.readUnsignedShort();
      boolean extended = (tagID & HDFConstants.DFTAG_EXTENDED) != 0;
      tagID &= HDFConstants.DFTAG_EXTENDED - 1;
      int refNo = inStream.readUnsignedShort();
      int offset = inStream.readInt();
      int length = inStream.readInt();
      if (tagID == HDFConstants.DFTAG_VERSION) {
        // Version
        dd = new DDVersion(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_NULL || tagID == 0) {
        // No data
        dd = new DDNull(this, tagID, refNo, offset, length);
      } else if (tagID == HDFConstants.DFTAG_SD) {
        // Scientific data
        dd = new DDScientificData(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_SDD) {
        // Scientific data dimension record
        dd = new DDScientificDDR(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_NT) {
        // Number type
        dd = new DDNumberType(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_COMPRESSED) {
        // Compressed data
        dd = new DDCompressedBlock(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_VH) {
        // Vdata header
        dd = new DDVDataHeader(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_VS) {
        // VSet
        dd = new DDVSet(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_VG) {
        // VGroup
        dd = new DDVGroup(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_NDG) {
        // Numeric data group
        dd = new DDNumericDataGroup(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_CHUNK) {
        // Data chunk
        dd = new DDChunkData(this, tagID, refNo, offset, length, extended);
      } else if (tagID == HDFConstants.DFTAG_LINKED) {
        // A linked block table
        dd = new DDLinkedBlock(this, tagID, refNo, offset, length, extended);
      } else {
        System.err.printf("Found an unknown %sblock <%d,%d> @%d\n", extended? "extended " : "", tagID, refNo, inStream.getPos());
        dd = new DDUnknown(this, tagID, refNo, offset, length, extended);
      }
      
      dataDescriptors.put(new DDID(dd.tagID, dd.refNo), dd);
    }
  }
  
  /**
   * For debugging purpose. Print all information in this file.
   * @throws IOException 
   */
  public void printAll() throws IOException {
    System.out.printf("Total of %d data descriptors\n", dataDescriptors.size());
    for (DataDescriptor dd : dataDescriptors.values()) {
      dd.lazyLoad();
      System.out.printf("<%d, %d>: %s\n", dd.tagID, dd.refNo, dd);
    }
  }

  @Override
  public void close() throws IOException {
    this.inStream.close();
  }

  /**
   * Finds and returns the first group that matches the given name.
   * @param desiredName - the desired name of the group to find
   * @return - a reference to the group if found, otherwise, null is returned.
   * @throws IOException 
   */
  public DDVGroup findGroupByName(String desiredName) throws IOException {
    for (Map.Entry<DDID, DataDescriptor> entry : dataDescriptors.entrySet()) {
      if (entry.getKey().tagID == HDFConstants.DFTAG_VG) {
        DDVGroup group = (DDVGroup)entry.getValue();
        String groupName = group.getName();
        if (groupName != null && groupName.equals(desiredName))
          return group;
      }
    }
    return null;
  }
  
  public DDVDataHeader findHeaderByName(String desiredName) throws IOException {
    for (Map.Entry<DDID, DataDescriptor> entry : dataDescriptors.entrySet()) {
      if (entry.getKey().tagID == HDFConstants.DFTAG_VH) {
        DDVDataHeader header = (DDVDataHeader)entry.getValue();
        String headerName = header.getName();
        if (headerName != null && headerName.equals(desiredName))
          return header;
      }
    }
    return null;
  }

  /**
   * Retrieves an element given its ID in the file (tagID + reference Number)
   * @param ddid
   * @return
   */
  DataDescriptor retrieveElementByID(DDID ddid) {
    return dataDescriptors.get(ddid);
  }
}
