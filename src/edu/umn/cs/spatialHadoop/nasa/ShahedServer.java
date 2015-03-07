/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.nasa.AggregateQuadTree.Node;

/**
 * @author Ahmed Eldawy
 *
 */
public class ShahedServer extends AbstractHandler {

  private static final Log LOG = LogFactory.getLog(ShahedServer.class);
  
  /**Mail server*/
  private static final String MAIL_HOST;
  /**Properties used to configure the mail server*/
  private static final Properties MAIL_PROPERTIES;

  /**Common parameters for all queries*/
  private OperationsParams commonParams;
  /**Email address to send from*/
  private String from;
  /**Username of the mail server*/
  final private String username;
  /**Password of the mail server*/
  final private String password;

  /**The base directory in which all datasets are stored*/
  private Path dataPath;

  private Path indexPath;
  
  static {
    MAIL_HOST = "mail.cs.umn.edu";
    
    // Mail properties that work with mail.cs.umn.edu
    MAIL_PROPERTIES = new Properties();
 
    MAIL_PROPERTIES.put("mail.smtp.starttls.enable", "true");
    MAIL_PROPERTIES.put("mail.smtp.auth", "true");
    
    // Use the following if you need SSL
    MAIL_PROPERTIES.put("mail.smtp.socketFactory.port", 465);
    MAIL_PROPERTIES.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
    MAIL_PROPERTIES.put("mail.smtp.socketFactory.fallback", "false");

    
    MAIL_PROPERTIES.put("mail.smtp.host", MAIL_HOST);
    MAIL_PROPERTIES.put("mail.smtp.port", "465");
  }

  /**
   * A constructor that starts the Jetty server
   * @param dataPath
   * @param indexPath
   * @param params
   */
  public ShahedServer(Path dataPath, Path indexPath, OperationsParams params) {
    this.commonParams = new OperationsParams(params);
    this.username = params.get("username");
    this.password = params.get("password");
    this.from = params.get("from", this.username);
    this.dataPath = dataPath;
    this.indexPath = indexPath;
  }

  /**
   * Create an HTTP web server (using Jetty) that will stay running to answer
   * all queries
   * @throws Exception 
   */
  private static void startServer(Path dataPath, Path indexPath,
      OperationsParams params) throws Exception {
    int port = params.getInt("port", 8889);

    Server server = new Server(port);
    server.setHandler(new ShahedServer(dataPath, indexPath, params));
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
    ((Request) request).setHandled(true);
    
    try {
      if (target.endsWith("/generate_image.cgi")) {
        LOG.info("Generating image");
        // Start a background thread that handles the request
        new ImageRequestHandler(request).start();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain;charset=utf-8");
        response.getWriter().println("Image request received successfully");
      } else if (target.endsWith("/aggregate_query.cgi")) {
        handleAggregateQuery(request, response);
        LOG.info("Query results returned");
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
   * Handle a request for a spatio-temporal aggregate query.
   * @param request
   * @param response
   * @throws ParseException
   * @throws IOException
   */
  private void handleAggregateQuery(HttpServletRequest request,
      HttpServletResponse response) throws ParseException, IOException {
    String west = request.getParameter("min_lon");
    String east = request.getParameter("max_lon");
    String south = request.getParameter("min_lat");
    String north = request.getParameter("max_lat");
    
    String[] startDateParts = request.getParameter("fromDate").split("/");
    String startDate = startDateParts[2] + '.' + startDateParts[0] + '.' + startDateParts[1];
    String[] endDateParts = request.getParameter("toDate").split("/");
    String endDate = endDateParts[2] + '.' + endDateParts[0] + '.' + endDateParts[1];
    
    // Create the query parameters
    OperationsParams params = new OperationsParams(commonParams);
    params.set("rect", west+','+south+','+east+','+north);
    params.set("time", startDate+".."+endDate);
    
    long t1 = System.currentTimeMillis();
    Node result = SpatioTemporalAggregateQuery.aggregateQuery(indexPath, params);
    long t2 = System.currentTimeMillis();
    // Report the answer and time
    response.setContentType("application/json;charset=utf-8");
    PrintWriter writer = response.getWriter();
    writer.print("{");
    writer.print("\"results\":{");
    writer.print("\"min\": "+result.min+',');
    writer.print("\"max\": "+result.max+',');
    writer.print("\"count\": "+result.count+',');
    writer.print("\"sum\": "+result.sum);
    writer.print("},");
    writer.print("\"stats\":{");
    writer.print("\"totaltime\":"+(t2-t1)+',');
    writer.print("\"num-of-temporal-partitions\":"+SpatioTemporalAggregateQuery.numOfTemporalPartitionsInLastQuery+',');
    writer.print("\"num-of-trees\":"+SpatioTemporalAggregateQuery.numOfTreesTouchesInLastRequest);
    writer.print("}");
    writer.print("}");
    response.setStatus(HttpServletResponse.SC_OK);
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
        getClass().getResourceAsStream("/webapps/static/shahedfrontend"+target);
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
    response.setContentType(URLConnection.guessContentTypeFromName(target));
  }
  
  private class ImageRequestHandler extends Thread {

    /**A unique ID for this request*/
    private String datasetPath;
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
    private Path outDir;

    public ImageRequestHandler(HttpServletRequest request) throws IOException {
      FileSystem fs = FileSystem.get(commonParams);
      do {
        this.outDir = new Path(String.format("%06d", (int)(Math.random() * 1000000)));
      } while (fs.exists(outDir));
      this.requesterName = request.getParameter("user_name");
      this.email = request.getParameter("email");
      this.datasetPath = request.getParameter("dataset_url");
      this.datasetName = request.getParameter("dataset");
      this.west = request.getParameter("min_lon");
      this.east = request.getParameter("max_lon");
      this.south = request.getParameter("min_lat");
      this.north = request.getParameter("max_lat");

      String[] startDateParts = request.getParameter("fromDate").split("/");
      this.startDate = startDateParts[2] + '.' + startDateParts[0] + '.' + startDateParts[1];

      // Create the query parameters
      this.rect = west+','+south+','+east+','+north;
    }
    
    @Override
    public void run() {
      try {
        sendConfirmEmail();
        byte[] imageFile = plotImage();
        byte[] kmlFile = createKMLFile();
        sendResponseEmail(imageFile, kmlFile);
      } catch (Exception e) {
        LOG.info("Error handling request");
        e.printStackTrace();
      }
    }

    /**
     * Sends an email that confirms the receipt of the request
     * @param target
     * @param request
     * @param requestID
     * @throws MessagingException 
     * @throws AddressException 
     * @throws UnsupportedEncodingException 
     */
    private void sendConfirmEmail() throws AddressException, MessagingException, UnsupportedEncodingException {
      Properties props = new Properties(MAIL_PROPERTIES);
      
      props.put("mail.smtp.user", from);
      props.put("mail.smtp.password", password);

      Session mailSession = Session.getDefaultInstance(props);
      
      Message message = new MimeMessage(mailSession);
      InternetAddress requesterAddress = new InternetAddress(email, requesterName);
      message.setFrom(new InternetAddress(username));
      message.addRecipient(RecipientType.TO, requesterAddress);
      InternetAddress adminAddress = new InternetAddress("eldawy@cs.umn.edu", "Ahmed Eldawy");
      message.addRecipient(RecipientType.BCC, adminAddress);
      message.setSubject("Confirmation: Your request was received");
      message.setText("Dear "+requesterName+",\n"+
          "Your request was received. "+
          "The server is currently processing your request and you will receive " +
          "an email with the generated files as soon as the request is complete.\n\n"+
          "Thank you for using Shahed. \n\n Shahed team");
      InternetAddress shahedAddress = new InternetAddress(from, "SHAHED Team");
      
      message.setFrom(shahedAddress);
      message.setReplyTo(new InternetAddress[] {shahedAddress});
      
      Transport transport = mailSession.getTransport();
      transport.connect(MAIL_HOST, username, password);
      
      transport.sendMessage(message, message.getAllRecipients());
      transport.close();
      LOG.info("Message sent successfully to '"+requesterAddress+"'");
    }

    /**
     * Plots the image as the user requested
     * @throws IOException
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    private byte[] plotImage() throws IOException, InterruptedException, ClassNotFoundException {
      this.inputURL = new Path(dataPath, datasetPath+"/"+startDate);
      Path outputPath = new Path(outDir, "image.png");
      // Launch the MapReduce job that plots the dataset
      OperationsParams plotParams = new OperationsParams(commonParams);
      plotParams.setBoolean("vflip", true);
      plotParams.setClass("shape", NASARectangle.class, Shape.class);
      plotParams.set("rect", rect);
      plotParams.setBoolean("recoverholes", true);
      plotParams.set("dataset", datasetName);
      
      HDFPlot2.plotHeatMap(new Path[] {inputURL}, outputPath, plotParams);
      
      FileSystem fs = outputPath.getFileSystem(commonParams);
      if (!fs.exists(outputPath)) {
        LOG.error("Image not generated");
        return null;
      }
      FSDataInputStream inputStream = fs.open(outputPath);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024*1024];
      int size;
      while ((size = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, size);
      }
      inputStream.close();
      baos.close();
      byte[] imageData = baos.toByteArray();
      fs.delete(outDir, true);
      return imageData;
    }

    /**
     * Creates a KML file that displays the generated image in Google Earth
     * @return
     */
    private byte[] createKMLFile() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter printWriter = new PrintWriter(baos);
      printWriter.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      printWriter.println("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
      printWriter.println("  <Folder>");
      printWriter.println("    <name>Generated Image</name>");
      printWriter.println("    <description>No describtion available</description>");
      printWriter.println("    <GroundOverlay>");
      printWriter.println("      <name></name>");
      printWriter.println("      <description>None available</description>");
      printWriter.println("      <Icon>");
      printWriter.println("        <href>image.png</href>");
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
      printWriter.close();
      return baos.toByteArray();
    }

    /**
     * Sends an email to the client with the generated image.
     * @param kmlBytes
     * @throws MessagingException 
     * @throws AddressException 
     * @throws IOException 
     */
    private void sendResponseEmail(byte[] imageBytes, byte[] kmlBytes) throws AddressException, MessagingException, IOException {
      Properties props = new Properties(MAIL_PROPERTIES);
      
      props.put("mail.smtp.user", from);
      props.put("mail.smtp.password", password);
    
      Session mailSession = Session.getInstance(props);
    
      Message message = new MimeMessage(mailSession);
      message.setFrom(new InternetAddress(username));
      String toLine = requesterName+'<'+email+'>';
      message.setRecipients(RecipientType.TO, InternetAddress.parse(toLine));
      message.setSubject("Your request is complete");
      
      Multipart multipart = new MimeMultipart();
      
      MimeBodyPart textPart = new MimeBodyPart();
      textPart.setText("Dear "+requesterName+",\n"+
          "Your request was successfully completed. "+
          "Please find the generated images attached.\n\n"+
          "Thank you for using Shahed. \n\n Shahed team");
      multipart.addBodyPart(textPart);
      
      MimeBodyPart imagePart = new MimeBodyPart();
      DataSource source1 = new ByteArrayDataSource(imageBytes, "image/png");
      imagePart.setDataHandler(new DataHandler(source1));
      imagePart.setFileName("image.png");
      multipart.addBodyPart(imagePart);
      
      // Create a KMZ file and attach it to the email
      ByteArrayOutputStream kmzFile = new ByteArrayOutputStream();
      ZipOutputStream zipOut = new ZipOutputStream(kmzFile);
      ZipEntry ze = new ZipEntry("image.png");
      zipOut.putNextEntry(ze);
      zipOut.write(imageBytes);
      zipOut.closeEntry();
      
      ze = new ZipEntry("heatmap.kml");
      zipOut.putNextEntry(ze);
      zipOut.write(kmlBytes);
      zipOut.closeEntry();
      
      zipOut.close();
      byte[] kmzBytes = kmzFile.toByteArray();

      MimeBodyPart kmzPart = new MimeBodyPart();
      DataSource source2 = new ByteArrayDataSource(kmzBytes, "application/vnd.google-earth.kmz");
      kmzPart.setDataHandler(new DataHandler(source2));
      kmzPart.setFileName("heatmap.kmz");
      multipart.addBodyPart(kmzPart);
      
      message.setContent(multipart);

      Transport transport = mailSession.getTransport();
      transport.connect(MAIL_HOST, username, password);
      
      transport.sendMessage(message, message.getAllRecipients());
      transport.close();
      LOG.info("Request finished successfully");
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
    System.out.println("<datasets path> - Path the raw MODIS files");
    System.out.println("<index path> - (*) Path the indexed modis data");
    System.out.println("username:<u> - (*) Username to authenticate with the mail server");
    System.out.println("password:<pw> - (*) Password to authenticate with the mail server");
    System.out.println("email:<pw> - (*) Email to send from");
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
    if (params.get("username") == null || params.get("password") == null) {
      System.err.println("Please specify username and password for mail server");
      printUsage();
      System.exit(1);
    }
    Path[] paths = params.getPaths();
    Path datasetPath = paths.length == 1? new Path("http://e4ftl01.cr.usgs.gov/") : paths[0];
    Path indexPath = paths.length == 1? paths[0] : paths[1];
    startServer(datasetPath, indexPath, params);
  }
}
