package org.apache.hadoop.io;

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
