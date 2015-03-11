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
 * A record that describes a special block.
 * @author Ahmed Eldawy
 *
 */
public class DDExtendedBlock extends DataDescriptor {

  /** Type of extension */
  protected int extensionType;
  
  /** Compression version for compressed blocks */
  protected int compressionVersion;

  /** Length of the data after it will be decompressed*/
  protected int lengthOfUncompressedData;

  /** Reference number of the compressed data*/
  protected int linkedRefNo;

  /**Currently on streaming I/O*/
  protected int modelType;

  /**Type of compression*/
  protected int compressionType;

  /**Deflate level of deflate compression*/
  protected int deflateLevel;

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    extensionType = in.readUnsignedShort();
    if (extensionType == HDFConstants.SPECIAL_COMP) {
      // Compressed block
      this.compressionVersion = in.readUnsignedShort();
      this.lengthOfUncompressedData = in.readInt();
      this.linkedRefNo = in.readUnsignedShort();
      this.modelType = in.readUnsignedShort();
      this.compressionType = in.readUnsignedShort();
      if (this.compressionType == HDFConstants.COMP_CODE_DEFLATE) {
        this.deflateLevel = in.readUnsignedShort();
      } else {
        System.err.println("Unsupported compression "+this.compressionType);
      }
    } else {
      System.err.println("Unsupported extension type "+extensionType);
    }
  }
  
  @Override
  public String toString() {
    if (extensionType == HDFConstants.SPECIAL_COMP) {
      return String.format("Compressed record of type %d with total size of %d for referenced block %d", compressionType, lengthOfUncompressedData, linkedRefNo);
    }
    return String.format("Unsupported linked record of type %d", extensionType);
  }
}
