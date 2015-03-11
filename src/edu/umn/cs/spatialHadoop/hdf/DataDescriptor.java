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
 * An abstract class for any data descriptor
 * @author Eldawy
 *
 */
public abstract class DataDescriptor {
  
  /**ID (type) of this tag*/
  public int tagID;
  
  /**Reference number. Unique ID within this tag*/
  public int refNo;
  
  /**Offset of this data descriptor in the HDF file*/
  public int offset;
  
  /**Number of bytes in this data descriptor*/
  public int length;

  public DataDescriptor() {
  }
  
  /**
   * Reads information of this data descriptor from the input.
   * @param in - data input from where to read information
   * @param length - total number of data bytes
   */
  public abstract void readFields(FSDataInputStream in) throws IOException;
  
  /**
   * Reads the raw data into a byte array
   * @param in
   * @return
   * @throws IOException
   */
  protected byte[] readRawData(FSDataInputStream in) throws IOException {
    byte[] rawData = new byte[length];
    in.seek(offset);
    in.readFully(rawData);
    return rawData;
  }

}
