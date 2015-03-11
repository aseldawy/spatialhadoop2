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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * A block that stores compressed data.
 * @author Ahmed Eldawy
 *
 */
public class DDCompressedBlock extends DataDescriptor {

  protected byte[] uncompressedData;
  
  public DDCompressedBlock() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    byte[] compressedData = readRawData(in);
    InputStream dis = new InflaterInputStream(new ByteArrayInputStream(compressedData));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int bufferLength;
    while ((bufferLength = dis.read(buffer)) > 0) {
      baos.write(buffer, 0, bufferLength);
    }
    dis.close();
    baos.close();
    uncompressedData = baos.toByteArray();
  }

  public String toString() {
    byte[] head = new byte[Math.min(uncompressedData.length, 64)];
    System.arraycopy(uncompressedData, 0, head, 0, head.length);
    return String.format("Successfully deflated %d bytes out of %d bytes of data for refNo %d", uncompressedData.length, length, refNo);
  }
}
