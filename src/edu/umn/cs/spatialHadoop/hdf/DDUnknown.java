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
 * A place holder for data descriptors with unsupported tag number
 * @author Eldawy
 *
 */
public class DDUnknown extends DataDescriptor {

  public byte[] rawData;
  
  public DDUnknown() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    rawData = new byte[length];
    in.seek(offset);
    in.readFully(rawData);
  }

  @Override
  public String toString() {
    byte[] head = new byte[Math.min(rawData.length, 16)];
    System.arraycopy(rawData, 0, head, 0, head.length);
    return String.format("Unknown tag %d with data of size %d '%s' ...", tagID, length, new String(head));
  }
}
