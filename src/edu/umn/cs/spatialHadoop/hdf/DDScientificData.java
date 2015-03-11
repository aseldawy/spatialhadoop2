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
 * Data descriptor for array of scientific data.
 * @author Eldawy
 *
 */
public class DDScientificData extends DataDescriptor {

  /**Raw data*/
  public byte[] data;

  public DDScientificData() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    data = new byte[length];
    in.seek(offset);
    in.readFully(data);
  }

  @Override
  public String toString() {
    byte[] head = new byte[Math.min(data.length, 16)];
    System.arraycopy(data, 0, head, 0, head.length);
    return String.format("Scientific data of size %d '%s' ...", length, new String(head));
  }
}
