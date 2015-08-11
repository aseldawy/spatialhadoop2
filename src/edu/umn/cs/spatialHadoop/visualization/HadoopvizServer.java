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
import java.text.ParseException;
import java.util.ArrayList;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.operations.GeometricPlot;
import edu.umn.cs.spatialHadoop.operations.HeatMapPlot;

public class HadoopvizServer extends AbstractHandler {

  private static final Log LOG = LogFactory.getLog(HadoopvizServer.class);


  /**Common parameters for all queries*/
  private OperationsParams commonParams;
  private Path datasetPath;
  private Path outputPath;
  
  /** Job Number **/
  private int jobNumber;
  
  /**
   * A constructor that starts the Jetty server
   * @param dataPath
   * @param indexPath
   * @param params
   */
  public HadoopvizServer(Path datasetPath, Path outputPath, OperationsParams params) {
    this.commonParams = new OperationsParams(params);
    this.datasetPath = datasetPath;
    this.outputPath = outputPath;
    this.jobNumber = Integer.MAX_VALUE;
  }

  /**
   * Create an HTTP web server (using Jetty) that will stay running to answer
   * all queries
   * @throws Exception 
   */
  private static void startServer(Path datasetPath, Path outputPath, OperationsParams params) throws Exception {
    int port = params.getInt("port", 8889);
    Server server = new Server(port);
    server.setHandler(new HadoopvizServer(datasetPath, outputPath, params));
    server.start();
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
    response.addHeader("Access-Control-Allow-Origin", "*");
    response.addHeader("Access-Control-Allow-Credentials", "true");
    ((Request) request).setHandled(true);
    
    try {
      LOG.info("Received request: '"+request.getRequestURL()+"'");
      if(target.endsWith("/generate_dataset.cgi")) {
        handleGenerateDataset(request, response);
      } else if(target.endsWith("/visualize.cgi")) {
        handlePlot(request, response);
      } else if(target.endsWith("/generate_output_list.cgi")) {
        handleGenerateOutputList(request, response);
      } else if(target.endsWith("/fetch_result.cgi")) {
        handleOutput(request, response);
      } else if(target.startsWith("/hdfs/")) {
        LOG.info("Fetching from HDFS");
        handleHDFSFetch(request, response);
      } else {
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
   * This will be called when the index.html loaded. It will return the avialable
   * datasets.
   * @param request
   * @param response
   * @throws ParseException
   * @throws IOException
   */
  private void handleGenerateDataset(HttpServletRequest request,
      HttpServletResponse response) throws ParseException, IOException {

    try {
      FileSystem fs = datasetPath.getFileSystem(commonParams);
      ArrayList<String> datasetList = new ArrayList<String>();
      // Get the file list.
      for(FileStatus fileStatus : fs.listStatus(datasetPath)) {
        if (fileStatus.isDir()) {
          String fileName = fileStatus.getPath().getName();
          datasetList.add(fileName);
        }
      }
      
      // Report the answer.
      LOG.info("Reporting the answer.");
      response.setContentType("application/json;charset=utf-8");
      PrintWriter writer = response.getWriter();
      writer.print("[");
      for(int i = 0; i < datasetList.size(); i++) {
        if(i == datasetList.size()-1) {
          writer.print("\"" + datasetList.get(i) + "\"");
        } else {
          writer.print("\"" + datasetList.get(i) + "\",");
        }
      }
      writer.print("]");
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      System.out.println("error happened");
      response.setContentType("text/plain;charset=utf-8");
      PrintWriter writer = response.getWriter();
      e.printStackTrace(writer);
      writer.close();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * Handle all plotting requests.
   * @param request
   * @param response
   * @throws ParseException
   * @throws IOException
   */
  private void handlePlot (HttpServletRequest request,
      HttpServletResponse response) throws ParseException, IOException {
    
    try {
      LOG.info(request.getQueryString());
      // Get the parameter.
      String dataset = request.getParameter("dataset");
      String width = request.getParameter("width");
      String height = request.getParameter("height");
      String partition = request.getParameter("partition");
      String vizType = request.getParameter("viztype");
      String plotType = request.getParameter("plottype");
      
      // Check the dataset and get the shape.
      String shape = "";
      if(dataset.equals("all_ways")) {
        shape = "osm";
      } else if (dataset.equals("all_nodes")) {
        shape = "edu.umn.cs.spatialHadoop.osm.OSMPoint";
      }
      
      // Create the path.
      Path[] inputPath = new Path[1];
      inputPath[0] = new Path(datasetPath.toString() + "/" + dataset);
      // First run. Need to check the next available output path.
      if(jobNumber == Integer.MAX_VALUE) {
        jobNumber = 0;
        FileSystem fs = outputPath.getFileSystem(commonParams);
        // Get the file list.
        for(FileStatus fileStatus : fs.listStatus(outputPath)) {
          if (fileStatus.isDir()) {
            jobNumber++;
          }
        }
      }
      Path outputFolder = new Path(outputPath.toString() + "/" + jobNumber);
      
      // Create the query parameters
      OperationsParams params = new OperationsParams(commonParams);
      if(plotType.equals("gplot")) {
        params.set("color", "red");
      }
      params.set("shape", shape);
  
      if(vizType.equals("single_level")) {
        params.set("width", width);
        params.set("height", height);
        params.set("partition", partition);
        outputFolder = new Path(outputFolder.toString() + "/result.png");
      } else {
        params.setBoolean("pyramid", true);
        params.set("tileWidth", width);
        params.set("tileHeight", height);
  
        String min_zoom = request.getParameter("min_zoom");
        String max_zoom = request.getParameter("max_zoom");
        
        if(min_zoom.equals("null") && !max_zoom.equals("null")) {
          params.set("levels", "" + (Integer.parseInt(max_zoom) + 1));
        } else if(!min_zoom.equals("null") && max_zoom.equals("null")) {
          params.set("levels", min_zoom + ".." + min_zoom);
        } else {
          params.set("levels", min_zoom + ".." + max_zoom);
        }
        
        // Checked the partition. If null, use default.
        if(!partition.equals("null")) {
          params.set("partition", partition);
        }
      }
      
      params.setBoolean("background", true);
      
      Job job = null;
      if(plotType.equals("gplot")) {
        job = GeometricPlot.plot(inputPath, outputFolder, params);
      } else if (plotType.equals("hplot")) {
        job = HeatMapPlot.plot(inputPath, outputFolder, params);
      }
      // Report the answer and time
      response.setContentType("application/json;charset=utf-8");
      PrintWriter writer = response.getWriter();
      writer.print("{\"job\":\"" + job.getJobID() + "\",");
      writer.println("\"url\":\"" + job.getTrackingURL()+"\",");
      writer.print("\"output\":" + "\"" + jobNumber + "\"}");
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
      jobNumber++;
    } catch (Exception e) {
      response.setContentType("text/plain;charset=utf-8");
      PrintWriter writer = response.getWriter();
      e.printStackTrace(writer);
      writer.close();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * This method will be called when display.html loaded. It will show the
   * available output to be shown.
   * @param request
   * @param response
   * @throws ParseException
   * @throws IOException
   */
  private void handleGenerateOutputList(HttpServletRequest request,
      HttpServletResponse response) throws ParseException, IOException {

    try {
      FileSystem fs = outputPath.getFileSystem(commonParams);
      ArrayList<String> outputList = new ArrayList<String>();
      // Get the file list.
      for(FileStatus fileStatus : fs.listStatus(outputPath)) {
        if (fileStatus.isDir()) {
          String fileName = fileStatus.getPath().getName();
          outputList.add(fileName);
        }
      }     
      // Report the answer.
      LOG.info("Reporting the answer.");
      response.setContentType("application/json;charset=utf-8");
      PrintWriter writer = response.getWriter();
      writer.print("[");
      for(int i = 0; i < outputList.size(); i++) {
        if(i == outputList.size()-1) {
          writer.print("\"" + outputList.get(i) + "\"");
        } else {
          writer.print("\"" + outputList.get(i) + "\",");
        }
      }
      writer.print("]");
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      System.out.println("error happened");
      response.setContentType("text/plain;charset=utf-8");
      PrintWriter writer = response.getWriter();
      e.printStackTrace(writer);
      writer.close();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * This method will handle the output result.
   * @param request
   * @param response
   */
  private void handleOutput(HttpServletRequest request,
      HttpServletResponse response) {
    try {
      FileSystem fs = outputPath.getFileSystem(commonParams);
      Path directoryPath = new Path(outputPath.toString() + "/" + request.getParameter("path"));
      
      // Get the file list.
      Path fileName = null;
      for(FileStatus fileStatus : fs.listStatus(directoryPath)) {
        if(fileStatus.getPath().getName().endsWith("png") || fileStatus.getPath().getName().endsWith("html")) {
          fileName = fileStatus.getPath();
          break;
        }
      }
      response.setContentType("text/html");
      PrintWriter writer = response.getWriter();
      // Single Level.
      if(fileName.getName().endsWith("png")) {
        writer.print("<img src=\"/hdfs" + fileName.toUri().getPath() + "\">");
      // Multi Level.
      } else {
        writer.print("<iframe src=\"/hdfs" + fileName.toUri().getPath() + "\" width=\"765\" height=\"614\">");
      }
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      System.out.println("error happened");
      e.printStackTrace();
      response.setContentType("text/plain;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * This method will handle each time a file need to be fetched from HDFS.
   * @param request
   * @param response
   */
  private void handleHDFSFetch(HttpServletRequest request,
      HttpServletResponse response) {
    try {
      FileSystem fs = outputPath.getFileSystem(commonParams);
      String path = request.getRequestURI().replace("/hdfs", "");
      Path filePath = new Path(path);
      
      LOG.info("Fetching from " + path);
      
      FSDataInputStream resource = null;
     
      resource = fs.open(filePath);
      
      if (resource == null) {
        reportError(response, "Cannot load resource '"+filePath+"'", null);
        return;
      }
      byte[] buffer = new byte[1024*1024];
      ServletOutputStream outResponse = response.getOutputStream();
      int size;
      while ((size = resource.read(buffer)) != -1) {
        outResponse.write(buffer, 0, size);
      }
      resource.close();
      outResponse.close();
      response.setStatus(HttpServletResponse.SC_OK);
      if(filePath.toString().endsWith("png")) {
        response.setContentType("image/png");
      }
    } catch (Exception e) {
      System.out.println("error happened");
      e.printStackTrace();
      response.setContentType("text/plain;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * Tries to load the given resource name frmo class path if it exists.
   * Used to serve static files such as HTML pages, images and JavaScript files.
   * @param target
   * @param response
   * @throws IOException
   */
  private void tryToLoadStaticResource(String target,
      HttpServletResponse response) throws IOException {
    LOG.info("Loading resource "+target);
    // Try to load this resource as a static page
    InputStream resource =
        getClass().getResourceAsStream("/webapps/static/hadoopviz"+target);
    if (resource == null) {
      reportError(response, "Cannot load resource '"+target+"'", null);
      return;
    }
    byte[] buffer = new byte[1024*1024];
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


  private void reportError(HttpServletResponse response, String msg,
      Exception e)
          throws IOException {
    if (e != null)
      e.printStackTrace();
    LOG.error(msg);
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    response.getWriter().println("{\"message\": '"+msg+"',");
    if (e != null) {
      response.getWriter().println("\"error\": '"+e.getMessage()+"',");
      response.getWriter().println("\"stacktrace\": [");
      for (StackTraceElement trc : e.getStackTrace()) {
        response.getWriter().println("'"+trc.toString()+"',");
      }
      response.getWriter().println("]");
    }
    response.getWriter().println("}");
  }

  /**
   * Prints the usage of starting the server.
   */
  public static void printUsage() {
    System.out.println("Starts a server which will handle visualization requests");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<dataset> - (*) The path of the dataset.");
    System.out.println("<output> - (*) The output path.");
    System.out.println("port:<p> - The port to start listening to. Default: 8889");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    final OperationsParams params =
        new OperationsParams(new GenericOptionsParser(args), false);
    
    if (!params.checkInput()) {
      System.err.println("Please specify the path of the dataset.");
      printUsage();
      System.exit(1);
    }
    Path datasetPath = params.getPaths()[0];
    Path outputPath = params.getPaths()[1];
    startServer(datasetPath, outputPath, params);
  }
}
