/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper around a stream that limits it to read a fixed number
 * of bytes.
 * @author Ahmed Eldawy
 *
 */
public class InputSubstream extends InputStream {

  private InputStream in;
  
  private long remainingBytes;

  /**
   * 
   */
  public InputSubstream(InputStream in, long length) {
    this.in = in;
    this.remainingBytes = length;
  }

  @Override
  public int read() throws IOException {
    if (remainingBytes > 0) {
      remainingBytes--;
      return in.read();
    }
    return -1;
  }

  @Override
  public int available() throws IOException {
    return (int) Math.min(remainingBytes, 1024 * 1024);
  }
  
  @Override
  public void close() throws IOException {
    in.close();
  }
  
  
}
