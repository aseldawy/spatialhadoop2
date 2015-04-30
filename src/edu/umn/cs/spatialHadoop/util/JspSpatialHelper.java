/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.net.NetUtils;

public class JspSpatialHelper {
  public static String jobTrackUrl(String requestUrl, Configuration conf, Job job) {
    // Create a link to the status of the running job
    String trackerAddress = conf.get("mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(trackerAddress);
    int cutoff = requestUrl.indexOf('/', requestUrl.lastIndexOf(':'));
    requestUrl = requestUrl.substring(0, cutoff);
    InetSocketAddress requestSocAddr = NetUtils.createSocketAddr(requestUrl);
    String address = "http://"+requestSocAddr.getHostName()+":"+infoSocAddr.getPort()+
      "/jobdetails.jsp?jobid="+job.getJobID()+"&amp;refresh=30";
    return address;
  }
  
  /**
   * Runs the given process and returns the result code. Feeds the given string
   * to the stdin of the run process. If stdout or stderr is non-null, they are
   * filled with the stdout or stderr of the run process, respectively.
   * If wait is set to true, the process is run in synchronous mode where we
   * wait until it is finished. Otherwise, this function call returns
   * immediately and leaves the process running in the background. In the later
   * case, stdout, stderr and the return value are not valid.
   * 
   * @param workingDir - The working directory to run the script. Set null for
   *   default.
   * @param cmd - The command line to run including all parameters
   * @param stdin - The string to feed to the stdin of the run process.
   * @param stdout - If non-null, the stdout of the process is fed here.
   * @param stderr - If non-null, the stderr of the process is fed here.
   * @param wait - Set to true to wait until the process exits.
   * @return
   * @throws IOException
   */
  public static int runProcess(File workingDir, String cmd, String stdin, Text stdout, Text stderr, boolean wait) throws IOException {
    new File("asdf").list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return false;
      }
    });
    Process process;
    if (workingDir == null)
      process = Runtime.getRuntime().exec(cmd);
    else
      process = Runtime.getRuntime().exec(cmd, null, workingDir);
    if (stdin != null) {
      PrintStream ps = new PrintStream(process.getOutputStream());
      ps.print(stdin);
      ps.close();
    }
    
    if (!wait)
      return 0;
    
    try {
      int exitCode = process.waitFor();
      byte[] buffer = new byte[4096];
      if (stdout != null) {
        stdout.clear();
        InputStream in = process.getInputStream();
        while (in.available() > 0) {
          int bytesRead = in.read(buffer);
          stdout.append(buffer, 0, bytesRead);
        }
        in.close();
      }
      if (stderr != null) {
        stderr.clear();
        InputStream err = process.getErrorStream();
        while (err.available() > 0) {
          int bytesRead = err.read(buffer);
          stderr.append(buffer, 0, bytesRead);
        }
        err.close();
      }
      return exitCode;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return 1;
    }
  }
}
