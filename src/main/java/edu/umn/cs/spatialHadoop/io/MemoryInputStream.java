/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class MemoryInputStream extends ByteArrayInputStream
implements Seekable, PositionedReadable {

  int originalOffset;
  
  public MemoryInputStream(byte[] buf, int offset, int length) {
    super(buf, offset, length);
    originalOffset = offset;
  }

  public MemoryInputStream(byte[] buf) {
    super(buf);
  }

  public long getPos() {
    return pos - originalOffset;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.mark = originalOffset;
    this.reset();
    this.skip(pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    System.arraycopy(buf, (int)(originalOffset+position), buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
