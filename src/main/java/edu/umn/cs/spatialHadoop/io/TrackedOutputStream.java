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
import java.io.OutputStream;

/**
 * An output stream that keeps track of number of bytes written
 * @author Ahmed Eldawy
 *
 */
public class TrackedOutputStream extends OutputStream {

  /**The underlying output stream*/
  private OutputStream rawOut;
  
  /**Number of bytes written to the output so far*/
  private long offset;
  
  public void write(int b) throws IOException {
    rawOut.write(b);
    this.offset++;
  }

  public int hashCode() {
    return rawOut.hashCode();
  }

  public boolean equals(Object obj) {
    return rawOut.equals(obj);
  }

  public void flush() throws IOException {
    rawOut.flush();
  }

  public void close() throws IOException {
    rawOut.close();
  }

  public TrackedOutputStream(OutputStream raw) {
    this.rawOut = raw;
  }
  
  public long getPos() {
    return offset;
  }
}
