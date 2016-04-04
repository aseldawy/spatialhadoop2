/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A {@link FileSystem} that is able to open HTTP files. Note that certain
 * features are not supported by the HTTP scheme by design. First, it is
 * read-only which makes all write functionality not supported. Second, it
 * cannot list contents of a directory. For listing, some web browsers provide a
 * default behavior that lists contents of a directory as HTTP links. We try to
 * use this feature as much as possible to simulate the list functionality.
 * 
 * The browsing feature in this class is designed specifically to browse LP DAAC
 * NASA archives. Although most of the implementation is generic, there are some
 * features that are hard-coded to work with LP DAAC and might not work with
 * other web sites.
 * 
 * @author Ahmed Eldawy
 * 
 */
public class HTTPFileSystem extends FileSystem {
  private static final String HTTP_RETRIES = "fs.http.retries";

  public static final Log LOG = LogFactory.getLog(HTTPFileSystem.class);
  
  /**Default HTTP port*/
  private static final int DEFAULT_PORT = 80;
  
  /**Default HTTP block size*/
  private static final int BLOCK_SIZE = 4096;

  /**URI for this FileSystem*/
  private URI uri;

  /**Current working directory*/
  private Path workingDir;

  /**How many times to try access a file if failed downloading it*/
  public static int retries;
  
  static {
    // Associate this class with http scheme in default configuration
    Configuration.addDefaultResource("spatial-default.xml");
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.http.host", null) : host;
    if (host == null) {
      throw new IOException("Invalid host specified");
    }
    conf.set("fs.http.host", host);

    // get port information from uri, (overrides info in conf)
    int port = uri.getPort();
    port = (port == -1) ? DEFAULT_PORT : port;
    conf.setInt("fs.http.host.port", port);

    setConf(conf);
    this.uri = uri;
    retries = conf.getInt(HTTP_RETRIES, 3);
  }
  
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    URL url = f.toUri().toURL();
    return new FSDataInputStream(new HTTPInputStream(url));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new RuntimeException("Unsupported method #append in HTTP");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new RuntimeException("Unsupported method #rename in HTTP");
  }

  @Override
  public boolean delete(Path f) throws IOException {
    throw new RuntimeException("Unsupported method #delete in HTTP");
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new RuntimeException("Unsupported method #delete in HTTP");
  }

  private static long parseSize(String size) {
    char lastChar = size.charAt(size.length()-1);
    if (!Character.isDigit(lastChar)) {
      size = size.substring(0, size.length() - 1);
    }
    double dsize = Double.parseDouble(size);
    switch (lastChar) {
    case 'G': case 'g': dsize *= 1024;
    case 'M': case 'm': dsize *= 1024;
    case 'K': case 'k': dsize *= 1024;
    }
    return (long)dsize;
  }
  
  private static long parseDateTime(String date, String time) {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyy HH:mm");
    try {
      return dateFormat.parse(date+" "+time).getTime();
    } catch (ParseException e) {
      return 0;
    }
  }
  
  /**
   * Lists all files and directories in a given Path that points to a directory.
   * While this function is written in a generic way, it was designed and tested
   * only with LP DAAC archives.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Vector<FileStatus> statuses = new Vector<FileStatus>();
    final Pattern httpEntryPattern = Pattern.compile("<a href=\"[^\"]+\">(.+)</a>\\s*(\\d+-\\w+-\\d+)\\s+(\\d+:\\d+)\\s+([\\d\\.]+[KMG]|-)");
    f = f.makeQualified(this);
    URL url = f.toUri().toURL();
    int retryCount = HTTPFileSystem.retries;
    BufferedReader inBuffer = null;
    try {
      while (inBuffer == null && retryCount-- > 0) {
        try {
          inBuffer = new BufferedReader(new InputStreamReader(url.openStream()));
        } catch (java.net.SocketException e) {
          if (retryCount == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retryCount);
          try { Thread.sleep(1000); } catch (InterruptedException e1) { }
        } catch (java.net.UnknownHostException e) {
          if (retryCount == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retryCount);
          try { Thread.sleep(1000); } catch (InterruptedException e1) { }
        }
      }
      if (inBuffer == null)
        throw new RuntimeException("Could not access URL "+f);
      String line;
      while ((line = inBuffer.readLine()) != null) {
        Matcher matcher = httpEntryPattern.matcher(line);
        while (matcher.find()) {
          String entryName = matcher.group(1);
          Path entryPath = new Path(f, entryName);

          String entryDate = matcher.group(2);
          String entryTime = matcher.group(3);
          long modificationTime = parseDateTime(entryDate, entryTime);

          String size = matcher.group(4);
          boolean isDir = size.equals("-");
          long length = isDir? 0 : parseSize(size);

          FileStatus fstatus = new FileStatus(length, isDir, 1, 4096,
              modificationTime, modificationTime, null, null, null, entryPath);
          statuses.add(fstatus);
        }
      }
    } finally {
      if (inBuffer != null)
        inBuffer.close();
    }

    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    this.workingDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new RuntimeException("Unsupported method #mkdirs in HTTP");
  }

  /**
   * Returns the status of a file. This method is designed specifically to work
   * with LP DAAC archive and will not work correctly with other web sites.
   * Since HTTP does not tell whether a URL points to a file or directory,
   * we assume that URLs ending with HDF, XML and JPG are files while anything
   * else is considered a directory.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    f = f.makeQualified(this);
    URL url = f.toUri().toURL();
    int retryCount = HTTPFileSystem.retries;
    
    HttpURLConnection connection = null;
    try {
      while (connection == null && retryCount-- > 0) {
        try {
          connection = (HttpURLConnection) url.openConnection();
        } catch (java.net.SocketException  e) {
          if (retryCount == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retryCount);
          try {;
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
          }
        } catch (java.net.UnknownHostException e) {
          if (retryCount == 0)
            throw e;
          LOG.info("Error accessing file '"+url+"'. Trials left: "+retryCount);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
          }
        }
      }
      
      if (connection == null)
        throw new RuntimeException("Could not connect to "+f);
      String lengthStr = connection.getHeaderField("content-Length");
      long length = lengthStr == null? -1 : Long.parseLong(lengthStr);
      if (length == -1)
        LOG.info("Unknown HTTP file length "+length);
      long modificationTime = connection.getLastModified();
      if (modificationTime == 0)
        modificationTime = connection.getDate();
      // Hard coded to work with LP DAAC archives
      boolean isdir = !f.getName().matches("(?i:([^*\\?])*\\.(hdf|xml|jpg|gz|bz2|zip|txt|csv|tsv)$)");
      return new FileStatus(length, isdir, 1, BLOCK_SIZE, modificationTime, 0,
          null, null, null, f);
    } finally {
      if (connection != null)
        connection.disconnect();
    }
  }
}
