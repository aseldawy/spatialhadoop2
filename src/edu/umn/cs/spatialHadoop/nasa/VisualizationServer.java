/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.GeometricPlot;

/**
 * @author Ahmed Eldawy
 *
 */
public class VisualizationServer extends AbstractHandler {

  private static final Log LOG = LogFactory.getLog(VisualizationServer.class);

  /**Common parameters for all queries*/
  private OperationsParams commonParams;
  final private Path localWorkingDir;
  /**Username of the mail server*/
  final private String username;
  /**Password of the mail server*/
  final private String password;
  /**Intermediate files in HDFS to generate images*/
  private Path hdfsWorkingDir;

  public VisualizationServer(OperationsParams params) {
    this.localWorkingDir = new Path(params.get("workdir"));
    this.hdfsWorkingDir = new Path("visulization-request");
    this.commonParams = new OperationsParams(params);
    this.username = params.get("username");
    this.password = params.get("password");
  }

  /**
   * Create an HTTP web server (using Jetty) that will stay running to answer
   * all queries
   * @throws Exception 
   */
  private static void startServer(Path indexPath, OperationsParams params) throws Exception {
    int port = params.getInt("port", 8889);

    Server server = new Server(port);
    server.setHandler(new VisualizationServer(params));
    server.start();
    server.join();
  }

  @Override
  public void handle(String target, HttpServletRequest request,
      HttpServletResponse response, int dispatch) throws IOException,
      ServletException {
    // Bypass cross-site scripting (XSS)
    response.addHeader("Access-Control-Allow-Origin", "*");
    response.addHeader("Access-Control-Allow-Credentials", "true");
    response.setContentType("application/json;charset=utf-8");
    ((Request) request).setHandled(true);

    if (target.equals("/generate_image")) {
      LOG.info("Generating image");
      // Generate a video
      int requestID = generateRequestID();
      // Start a background thread that handles the request
      new ImageRequestHandler(request, requestID).start();
    }
  }
  
  private class ImageRequestHandler extends Thread {

    /**A unique ID for this request*/
    private int requestID;
    private String datasetURL;
    private String requesterName;
    private String email;
    private String datasetName;
    private String startDate;
    private String rect;
    private String west;
    private String east;
    private String south;
    private String north;
    private Path inputURL;
    private Path outputPath;

    public ImageRequestHandler(HttpServletRequest request, int requestID) {
      this.requestID = requestID;
      this.requesterName = request.getParameter("user_name");
      this.email = request.getParameter("email");
      this.datasetURL = request.getParameter("dataset_url");
      this.datasetName = request.getParameter("dataset");
      this.west = request.getParameter("min_lon");
      this.east = request.getParameter("max_lon");
      this.south = request.getParameter("min_lat");
      this.north = request.getParameter("max_lat");

      String[] startDateParts = request.getParameter("fromDate").split("/");
      this.startDate = startDateParts[2] + '.' + startDateParts[0] + '.' + startDateParts[1];

      // Create the query parameters
      this.rect = west+','+south+','+north+','+east;
    }
    
    @Override
    public void run() {
      try {
        sendConfirmEmail();
        plotImage();
        // Create a KML file for this request
//        String kmlFile = createKMLFile();
        
        // Send a notification email with the result

      } catch (Exception e) {
        LOG.info("Error handling request");
        e.printStackTrace();
      }
    }

    /*private String createKMLFile() {
      PrintWriter printWriter = new PrintWriter(new ByteArrayOutputStream());
      printWriter.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      printWriter.println("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
      printWriter.println("  <Folder>");
      printWriter.println("    <name>Generated Image</name>");
      printWriter.println("    <description>No describtion available</description>");
      printWriter.println("    <GroundOverlay>");
      printWriter.println("      <name></name>");
      printWriter.println("      <description>None available</description>");
      printWriter.println("      <Icon>");
      printWriter.println("        <href>"+outputPath.getName()+"</href>");
      printWriter.println("      </Icon>");
      printWriter.println("      <LatLonBox>");
      printWriter.println("        <north>"+ north +"</north>");
      printWriter.println("        <south>"+ south +"</south>");
      printWriter.println("        <east>"+ east +"</east>");
      printWriter.println("        <west>"+ west +"</west>");
      printWriter.println("        <rotation>0</rotation>");
      printWriter.println("      </LatLonBox>");
      printWriter.println("    </GroundOverlay>");
      printWriter.println("  </Folder>");
      printWriter.println("</kml>");
      
    }*/

    /**
     * Plots the image as the user requested
     * @throws IOException
     */
    private void plotImage() throws IOException {
      this.inputURL = new Path(datasetURL+"/"+startDate);
      this.outputPath = new Path(hdfsWorkingDir, String.format("%05d/image-%05d.png", requestID, requestID));

      // Launch the MapReduce job that plots the dataset
      OperationsParams plotParams = new OperationsParams();
      plotParams.setBoolean("vflip", true);
      plotParams.setClass("shape", NASARectangle.class, Shape.class);
      plotParams.set("rect", rect);
      plotParams.setBoolean("recoverholes", true);
      plotParams.set("dataset", datasetName);
      
      GeometricPlot.plotMapReduce(inputURL, outputPath, plotParams);
    }

    /**
     * Sends an email that confirms the receipt of the request
     * @param target
     * @param request
     * @param requestID
     * @throws MessagingException 
     * @throws AddressException 
     */
    private void sendConfirmEmail() throws AddressException, MessagingException {
      Properties props = new Properties();
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.host", "smtp.gmail.com");
      props.put("mail.smtp.port", "587");

      Session session = Session.getInstance(props,
          new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, password);
        }
      });

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(username));
      String toLine = requesterName+'<'+email+'>';
      message.setRecipients(RecipientType.TO, InternetAddress.parse(toLine));
      message.setSubject("Confirmation: Your request was received");
      message.setText("Dear "+requesterName+",\n"+
          "Your request was received and assigned the ID #"+requestID+". "+
          "The server is currently processing your request and you will received " +
          "an email with the generated files as soon as it is complete.\n\n"+
          "Thank you for using Shahed. \n\n Shahed team");
      Transport.send(message);
      LOG.info("Message sent successfully to '"+toLine+"'");
    }
  }


  private int generateRequestID() throws IOException {
    FileSystem fs = localWorkingDir.getFileSystem(commonParams);
    FileStatus[] prevFiles = fs.listStatus(localWorkingDir);
    int maxID = 0;
    for (FileStatus prevFile : prevFiles) {
      int prevID = Integer.parseInt(prevFile.getPath().getName());
      if (prevID > maxID)
        maxID = prevID;
    }
    return maxID + 1;
  }

  /**
   * Report an error back to the web browser
   * @param response
   * @param msg
   * @param e
   * @throws IOException
   */
  private static void reportError(HttpServletResponse response, String msg,
      Exception e)
          throws IOException {
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    response.getWriter().println("{error: '"+e.getMessage()+"',");
    response.getWriter().println("message: '"+msg+"',");
    response.getWriter().println("stacktrace: [");
    for (StackTraceElement trc : e.getStackTrace()) {
      response.getWriter().println("'"+trc.toString()+"',");
    }
    response.getWriter().println("]");
    response.getWriter().println("}");
  }

  /**
   * Prints the usage of starting the server.
   */
  public static void printUsage() {
    System.out.println("Starts a server which will handle visualization requests");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("workdir:<w> - (*) The working directory where all intermediate files are placed");
    System.out.println("username:<u> - (*) Username to authenticate with the mail server");
    System.out.println("password:<pw> - (*) Password to authenticate with the mail server");
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
    if (params.get("workdir") == null) {
      System.err.println("Error! Working directory not set");
      printUsage();
      System.exit(1);
    }
    startServer(params.getInputPath(), params);
  }
}
