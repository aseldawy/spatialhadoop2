/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.ByteArrayOutputStream;

public class MemoryOutputStream extends ByteArrayOutputStream {
  
  public MemoryOutputStream(byte[] buffer) {
    this.buf = buffer;
    this.count = 0;
  }

  public int getLength() {
    return count;
  }

  public void clear() {
    count = 0;
  }

}
