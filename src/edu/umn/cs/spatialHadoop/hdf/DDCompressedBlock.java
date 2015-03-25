/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * A block that stores compressed data.
 * TagID DFTAG_COMPRESSED
 * @author Ahmed Eldawy
 *
 */
public class DDCompressedBlock extends DataDescriptor {

  DDCompressedBlock(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    // This method is not expected to be called directly because this block
    // does not have the necessary compression information to decompress
    // the raw data correctly
    System.err.println("This method should never be called directly on compressed blocks");
  }
  
  protected InputStream decompressDeflate(int level) throws IOException {
    // We need to retrieve the raw data first and then pass it to the
    // decompressor. Otherwise, the decompressor will not be able to determine
    // the end-of-file
    // TODO try to create a wrapper stream that reads only the correct amount
    // of bytes without having to load everything in memory
    byte[] rawData = new byte[getLength()];
    hdfFile.inStream.seek(offset);
    hdfFile.inStream.readFully(rawData);
    InputStream decompressedData =
        new InflaterInputStream(new ByteArrayInputStream(rawData));
    return decompressedData;
  }

  public String toString() {
    return String.format("Compressed block <%d, %d>", tagID, refNo);
  }
}
