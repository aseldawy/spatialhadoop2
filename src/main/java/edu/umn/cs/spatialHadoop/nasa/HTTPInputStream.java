/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * A wrapper around a stream obtained from {@link URL#openStream()} that
 * makes it {@link Seekable} and {@link PositionedReadable} to be used
 * with {@link FSDataInputStream}, hence {@link HTTPFileSystem}.
 * 
 * All methods are delegated to the underlying (wrapped) input stream.
 * Non-supported methods raise an exception if called.
 * 
 * 
 * @author Ahmed Eldawy
 *
 */
public class HTTPInputStream extends InputStream implements Seekable, PositionedReadable {
  public static final Log LOG = LogFactory.getLog(HTTPInputStream.class);
  
  /**The underlying stream obtained from {@link URL#openStream()}*/
  private InputStream in;
  
  /**Cached value of content length.*/
  private long length;
  
  /**Current position in the file*/
  private long pos;

  /**The underlying URL. Used to reposition the stream on seeks*/
  private URL url;

  /**The underlying connection*/
  private HttpURLConnection conn;
  
  public HTTPInputStream(URL url) {
    this.url = url;
    this.pos = 0;
    this.length = -1; // Initially invalidate content length.
  }

  /**
   * Lazily opens a stream to the file if needed
   * @throws IOException 
   */
  private void lazyConnect() throws IOException {
    if (in == null) {
      if (conn == null) {
        int retries = Math.max(1, HTTPFileSystem.retries);
        while (conn == null && retries-- > 0) {
          try {
            conn = (HttpURLConnection) url.openConnection();
          } catch (java.net.SocketException e) {
            if (retries == 0)
              throw e;
            LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
          } catch (java.net.UnknownHostException e) {
            if (retries == 0)
              throw e;
            LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
          }
        }
      }
      
      if (pos != 0) {
        // Need to reposition the stream at the middle
        long fileLength = getContentLength();
        conn.setRequestProperty("Range", String.format("bytes %d-%d/%d", pos, fileLength, fileLength));
      }
      int retries = Math.max(1, HTTPFileSystem.retries);
      while (in == null && retries-- > 0) {
        try {
          this.in = conn.getInputStream();
        } catch (java.net.SocketException e) {
          if (retries == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        } catch (java.net.UnknownHostException e) {
          if (retries == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }
      }
      if (in == null)
        throw new IOException("Could not open an HTTP connection");
    }
  }

  public int read() throws IOException {
    lazyConnect();
    int value = in.read();
    pos++;
    return value;
  }

  public int read(byte[] b) throws IOException {
    lazyConnect();
    int diff = in.read(b);
    pos += diff;
    return diff;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int retries = HTTPFileSystem.retries;
    int diff;
    do {
      lazyConnect();
      diff = in.read(b, off, len);
      pos += diff;
    } while (retries-- > 0 && diff < 0 && pos < getContentLength());
    return diff;
  }

  public long skip(long n) throws IOException {
    long skipped = in.skip(n);
    pos += skipped;
    return skipped;
  }

  public String toString() {
    return in.toString();
  }

  public int available() throws IOException {
    return in.available();
  }

  public void close() throws IOException {
    if (in != null)
      in.close();
    if (conn != null)
      conn.disconnect();
  }

  public void mark(int readlimit) {
    in.mark(readlimit);
  }

  public void reset() throws IOException {
    in.reset();
  }

  public boolean markSupported() {
    return in.markSupported();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    return read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    seek(position);
    read(buffer);
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < pos || in == null) {
      // Reposition the stream and invalidate the underlying stream
      pos = newPos;
      if (this.in != null) {
        this.in.close();
      }
      this.in = null;
      if (this.conn != null) {
        this.conn.disconnect();
      }
      this.conn = null;
    } else {
      // Skip the difference and the #skip function will increment 'pos'
      skip(newPos - pos);
    }
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    // Currently, we support only one URL
    // If we have mirrors, we can do a connection to a new mirror here
    return false;
  }
  
  private long getContentLength() throws IOException {
    if (length < 0) {
      int retries = Math.max(1, HTTPFileSystem.retries);
      HttpURLConnection localConn = null;
      while (localConn == null && retries-- > 0) {
        try {
          localConn = (HttpURLConnection) url.openConnection();
        } catch (java.net.SocketException e) {
          if (retries == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
        } catch (java.net.UnknownHostException e) {
          if (retries == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retries);
        }
      }
      length = localConn.getContentLength();
      localConn.disconnect();
    }
    return length;
  }

}
