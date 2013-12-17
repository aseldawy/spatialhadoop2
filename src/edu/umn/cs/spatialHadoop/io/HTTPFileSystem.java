/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A {@link FileSystem} that is able to open HTTP files. Note that certain
 * features are not supported by the HTTP scheme by design. First, it is read-only
 * which makes all write functionality not supported. Second, it cannot list
 * contents of a directory. For listing, some web browsers provide a default
 * behavior that lists contents of a directory as HTTP links. We try to use
 * this feature as much as possible to simulate the list functionality.
 * 
 * @author Ahmed Eldawy
 *
 */
public class HTTPFileSystem extends FileSystem {
  public static final Log LOG = LogFactory.getLog(HTTPFileSystem.class);
  
  /**Default HTTP port*/
  private static final int DEFAULT_PORT = 80;
  
  /**Default HTTP block size*/
  private static final int BLOCK_SIZE = 4096;

  /**URI for this FileSystem*/
  private URI uri;

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
  }
  
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    URL url = f.toUri().toURL();
    InputStream in = url.openStream();
    return new FSDataInputStream(new HTTPInputStream(in));
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
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  @Override
  public boolean delete(Path f) throws IOException {
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  private static long parseSize(String size) {
    return 0;
  }
  
  private static long parseDateTime(String date, String time) {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyy HH:mm");
    try {
      return dateFormat.parse(date+" "+time).getTime();
    } catch (ParseException e) {
      return 0;
    }
  }
  
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Vector<FileStatus> statuses = new Vector<FileStatus>();
    final Pattern httpEntryPattern = Pattern.compile("<a href=\"[^\"]+\">(.+)</a>\\s*(\\d+-\\w+-\\d+)\\s+(\\d+:\\d+)\\s+([\\d\\.]+[KMG]|-)");
    URL url = f.toUri().toURL();
    InputStream inStream = url.openStream();
    BufferedReader inBuffer = new BufferedReader(new InputStreamReader(inStream));
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

    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    // TODO Auto-generated method stub

  }

  @Override
  public Path getWorkingDirectory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new RuntimeException("Unsupported method #create in HTTP");
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    URL url = f.toUri().toURL();
    URLConnection connection = url.openConnection();
    String lengthStr = connection.getHeaderField("content-Length");
    long length = lengthStr == null? 0 : Long.parseLong(lengthStr);
    long modificationTime = connection.getLastModified();
    if (modificationTime == 0)
      modificationTime = connection.getDate();
    boolean isdir = url.getPath().endsWith("/");
    return new FileStatus(length, isdir, 1, BLOCK_SIZE, modificationTime, 0,
        null, null, null, f);
  }

}
