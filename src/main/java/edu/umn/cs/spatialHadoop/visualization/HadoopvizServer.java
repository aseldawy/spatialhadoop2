/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which 
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLConnection;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.OperationsParams;

/**
 * A class that starts a web service that can visualize spatial data
 * @author Chrisopher Jonathan, Ahmed Eldawy
 *
 */
public class HadoopvizServer extends AbstractHandler {

  private static final Log LOG = LogFactory.getLog(HadoopvizServer.class);

  /**The name of the configuration line that stores the HTTP port*/
  private static final String HadoopVizWebServerPort =
      "spatialhadoop.hadoopviz.http_port";

  /** Common parameters for all queries */
  private OperationsParams commonParams;

  /**
   * A constructor that starts the Jetty server
   * @param params
   */
  public HadoopvizServer(OperationsParams params) {
    this.commonParams = new OperationsParams(params);
  }

  /**
   * Create an HTTP web server (using Jetty) that will stay running to answer
   * all queries
   * 
   * @throws Exception
   */
  private static void startServer(OperationsParams params) throws Exception {
    int port = params.getInt(HadoopVizWebServerPort, 8889);
    Server server = new Server(port);
    server.setHandler(new HadoopvizServer(params));
    server.start();
    LOG.info("HadoopViz server is running port: "+port);
    server.join();
  }

  /**
   * The handler for all requests.
   */
  @Override
  public void handle(String target, HttpServletRequest request,
      HttpServletResponse response, int dispatch) throws IOException,
      ServletException {
    // Bypass cross-site scripting (XSS)
    //response.addHeader("Access-Control-Allow-Origin", "*");
    //response.addHeader("Access-Control-Allow-Credentials", "true");
    ((Request) request).setHandled(true);

    try {
      LOG.info("Received request: '" + request.getRequestURL() + "'");
      if (target.startsWith("/hdfs/")) {
        handleHDFSFetch(request, response);
      } else if (target.endsWith("/LISTSTATUS.cgi")){
        handleListFiles(request, response);
      } else {
        // Doesn't match any of the dynamic content, assume it's a static file
        if (target.equals("/"))
          target = "/index.html";
        tryToLoadStaticResource(target, response);
      }
    } catch (Exception e) {
      e.printStackTrace();
      reportError(response, "Error placing the request", e);
    }
  }
  
  /**
   * Lists the contents of a directory
   * @param request
   * @param response
   */
  private void handleListFiles(HttpServletRequest request,
      HttpServletResponse response) {
    try {
      String pathStr = request.getParameter("path");
      Path path = new Path(pathStr == null? "/" : pathStr);
      FileSystem fs = path.getFileSystem(commonParams);
      FileStatus[] fileStatuses = fs.listStatus(path);
      PrintWriter out = response.getWriter();
      out.print("{\"FileStatuses\":{");
      out.printf("\"BaseDir\":\"%s\",", path);
      if (path.getParent() != null)
        out.printf("\"ParentDir\":\"%s\",", path.getParent());
      out.print("\"FileStatus\":[");
      for (FileStatus fileStatus : fileStatuses) {
        out.printf("{\"accessTime:%d,\"blockSize\":%d,\"childrenNum\":%d,\"fileId\":%d,"
            + "\"group\":\"%s\",\"length\":%d,\"modificationTime\":%d,"
            + "\"owner\":\"%s\",\"pathSuffix\":\"%s\",\"permission\":\"%s\","
            + "\"replication\":%d,\"storagePolicy\":%d,\"type\":\"%s\"},",
            fileStatus.getAccessTime(), fileStatus.getBlockSize(),
            0, 0, fileStatus.getGroup(), fileStatus.getLen(),
            fileStatus.getModificationTime(), fileStatus.getOwner(),
            fileStatus.getPath().getName(), fileStatus.getPermission(),
            fileStatus.getReplication(), 0,
            fileStatus.isDirectory()? "DIRECTORY" : "FILE");
      }
      out.print("]}}");
      out.close();
      response.setContentType("application/json;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      System.out.println("error happened");
      try {
        e.printStackTrace(response.getWriter());
      } catch (IOException ioe) {
        ioe.printStackTrace();
        e.printStackTrace();
      }
      response.setContentType("text/plain;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * This method will handle each time a file need to be fetched from HDFS.
   * 
   * @param request
   * @param response
   */
  private void handleHDFSFetch(HttpServletRequest request,
      HttpServletResponse response) {
    try {
      String path = request.getRequestURI().replace("/hdfs", "");
      Path filePath = new Path(path);
      FileSystem fs = filePath.getFileSystem(commonParams);

      LOG.info("Fetching from " + path);

      FSDataInputStream resource;

      resource = fs.open(filePath);

      if (resource == null) {
        reportError(response, "Cannot load resource '" + filePath + "'", null);
        return;
      }
      byte[] buffer = new byte[1024 * 1024];
      ServletOutputStream outResponse = response.getOutputStream();
      int size;
      while ((size = resource.read(buffer)) != -1) {
        outResponse.write(buffer, 0, size);
      }
      resource.close();
      outResponse.close();
      response.setStatus(HttpServletResponse.SC_OK);
      if (filePath.toString().endsWith("png")) {
        response.setContentType("image/png");
      }
    } catch (Exception e) {
      System.out.println("error happened");
      try {
        e.printStackTrace(response.getWriter());
      } catch (IOException ioe) {
        ioe.printStackTrace();
        e.printStackTrace();
      }
      response.setContentType("text/plain;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Tries to load the given resource name from class path if it exists. Used to
   * serve static files such as HTML pages, images and JavaScript files.
   * 
   * @param target
   * @param response
   * @throws IOException
   */
  private void tryToLoadStaticResource(String target,
      HttpServletResponse response) throws IOException {
    LOG.info("Loading resource " + target);
    // Try to load this resource as a static page
    InputStream resource = getClass().getResourceAsStream(
        "/webapps/static/hadoopviz" + target);
    if (resource == null) {
      reportError(response, "Cannot load resource '" + target + "'", null);
      return;
    }
    byte[] buffer = new byte[1024 * 1024];
    ServletOutputStream outResponse = response.getOutputStream();
    int size;
    while ((size = resource.read(buffer)) != -1) {
      outResponse.write(buffer, 0, size);
    }
    resource.close();
    outResponse.close();
    response.setStatus(HttpServletResponse.SC_OK);
    if (target.endsWith(".js")) {
      response.setContentType("application/javascript");
    } else if (target.endsWith(".css")) {
      response.setContentType("text/css");
    } else {
      response.setContentType(URLConnection.guessContentTypeFromName(target));
    }
  }

  private void reportError(HttpServletResponse response, String msg, Exception e)
      throws IOException {
    if (e != null)
      e.printStackTrace();
    LOG.error(msg);
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    response.getWriter().println("{\"message\": '" + msg + "',");
    if (e != null) {
      response.getWriter().println("\"error\": '" + e.getMessage() + "',");
      response.getWriter().println("\"stacktrace\": [");
      for (StackTraceElement trc : e.getStackTrace()) {
        response.getWriter().println("'" + trc.toString() + "',");
      }
      response.getWriter().println("]");
    }
    response.getWriter().println("}");
  }

  private String humanReadable(double size) {
    final String[] units = { "", "KB", "MB", "GB", "TB", "PB" };
    int unit = 0;
    while (unit < units.length && size > 1024) {
      size /= 1024;
      unit++;
    }
    return String.format("%.2f %s", size, units[unit]);
  }

  /**
   * Prints the usage of starting the server.
   */
  public static void printUsage() {
    System.out
        .println("Starts a server which will handle visualization requests");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final OperationsParams params = new OperationsParams(
        new GenericOptionsParser(args), false);
    startServer(params);
  }
}
