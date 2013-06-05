package org.apache.hadoop.io;

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
