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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
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
    MAIL_HOST = "smtp.gmail.com";
    
    // Mail properties that work with mail.cs.umn.edu
    MAIL_PROPERTIES = new Properties();

    MAIL_PROPERTIES.put("mail.smtp.auth", "true");
    MAIL_PROPERTIES.put("mail.smtp.starttls.enable", "true");
    MAIL_PROPERTIES.put("mail.smtp.port", "587");

    MAIL_PROPERTIES.put("mail.smtp.host", MAIL_HOST);
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
    if (params.get("shape") == null)
      params.setClass("shape", NASARectangle.class, Shape.class);

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
      LOG.info("Received request: '"+request.getRequestURL()+"'");
      if (target.endsWith("/generate_image.cgi")) {
        LOG.info("Generating image");
        // Start a background thread that handles the request
        new ImageRequestHandler(request).start();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain;charset=utf-8");
        response.getWriter().println("Image request received successfully");
      } else if (target.endsWith("/aggregate_query.cgi")) {
        handleAggregateQuery(request, response);
        LOG.info("Aggregate query results returned");
      } else if (target.endsWith("/selection_query.cgi")) {
        handleSelectionQuery(request, response);
        LOG.info("Selection query results returned");
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
    try {
      String west = request.getParameter("min_lon");
      String east = request.getParameter("max_lon");
      String south = request.getParameter("min_lat");
      String north = request.getParameter("max_lat");
      
      String[] startDateParts = request.getParameter("fromDate").split("/");
      String startDate = startDateParts[2] + '.' + startDateParts[0] + '.' + startDateParts[1];
      String[] endDateParts = request.getParameter("toDate").split("/");
      String endDate = endDateParts[2] + '.' + endDateParts[0] + '.' + endDateParts[1];
      LOG.info("Date range "+startDate+", "+endDate);
      
      // Create the query parameters
      OperationsParams params = new OperationsParams(commonParams);
      params.set("rect", west+','+south+','+east+','+north);
      params.set("time", startDate+".."+endDate);
      
      long t1 = System.currentTimeMillis();
      Node result = SpatioAggregateQueries.aggregateQuery(indexPath, params);
      long t2 = System.currentTimeMillis();
      // Report the answer and time
      response.setContentType("application/json;charset=utf-8");
      PrintWriter writer = response.getWriter();
      writer.print("{");
      writer.print("\"results\":{");
      if (result != null) {
        writer.print("\"min\": "+result.min+',');
        writer.print("\"max\": "+result.max+',');
        writer.print("\"count\": "+result.count+',');
        writer.print("\"sum\": "+result.sum);
      }
      writer.print("},");
      writer.print("\"stats\":{");
      writer.print("\"totaltime\":"+(t2-t1)+',');
      writer.print("\"num-of-temporal-partitions\":"+SpatioAggregateQueries.numOfTemporalPartitionsInLastQuery+',');
      writer.print("\"num-of-trees\":"+SpatioAggregateQueries.numOfTreesTouchesInLastRequest);
      writer.print("}");
      writer.print("}");
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      response.setContentType("text/plain;charset=utf-8");
      PrintWriter writer = response.getWriter();
      e.printStackTrace(writer);
      writer.close();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
  
  /**
   * Handle a request for a spatio-temporal aggregate query.
   * @param request
   * @param response
   * @throws ParseException
   * @throws IOException
   */
  private void handleSelectionQuery(HttpServletRequest request,
      HttpServletResponse response) throws ParseException, IOException {
    try {
      LOG.info("fromDate: '"+request.getParameter("fromDate")+"', toDate: '"+request.getParameter("toDate")+"'");
      final String lat = request.getParameter("lat");
      final String lon = request.getParameter("long");
      
      String[] startDateParts = request.getParameter("fromDate").split("/");
      String startDate = startDateParts[2] + '.' + startDateParts[1] + '.' + startDateParts[0];
      String[] endDateParts = request.getParameter("toDate").split("/");
      String endDate = endDateParts[2] + '.' + endDateParts[1] + '.' + endDateParts[0];
      
      // Create the query parameters
      OperationsParams params = new OperationsParams(commonParams);
      params.set("point", lon+","+lat);
      params.set("time", startDate+".."+endDate);
      
      final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
      final PrintWriter writer = response.getWriter();
      response.setContentType("application/json;charset=utf-8");
      writer.print("{");
      writer.print("\"results\":{");
      writer.print("\"points\":[");
      long t1 = System.currentTimeMillis();
      long numOfResults = SpatioAggregateQueries.selectionQuery(indexPath, new ResultCollector<NASAPoint>() {
        @Override
        public void collect(NASAPoint r) {
          synchronized (writer) {
            writer.printf("{\"lat\":%f, \"lon\":%f, \"timestamp\":\"%s\", \"value\":\"%d\"},",
                r.y, r.x, dateFormat.format(r.timestamp), r.getValue());
          }
        }
      }, params);
      long t2 = System.currentTimeMillis();
      writer.print("],");
      writer.printf("\"result-size\":%d},", numOfResults);
      writer.print("\"stats\":{");
      writer.print("\"totaltime\":"+(t2-t1)+',');
      writer.print("\"num-of-temporal-partitions\":"+SpatioAggregateQueries.numOfTemporalPartitionsInLastQuery+',');
      writer.print("\"num-of-trees\":"+SpatioAggregateQueries.numOfTreesTouchesInLastRequest);
      writer.print("}");
      writer.print("}");
      writer.close();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      response.setContentType("text/plain;charset=utf-8");
      PrintWriter writer = response.getWriter();
      e.printStackTrace(writer);
      writer.close();
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
    if (target.endsWith(".js")) {
      response.setContentType("application/javascript");
    } else if (target.endsWith(".css")) {
      response.setContentType("text/css");
    } else {
      response.setContentType(URLConnection.guessContentTypeFromName(target));
    }
    final DateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ");
    final long year = 1000L * 60 * 60 * 24 * 365;
    // Expires in a year
    response.addHeader("Expires", format.format(new Date().getTime() + year));
  }
  
  private class ImageRequestHandler extends Thread {

    /**A unique ID for this request*/
    private String datasetPath;
    private String requesterName;
    private String email;
    private String datasetName;
    /**Start and end dates*/
    private long startDate, endDate;
    private String rect;
    private String west;
    private String east;
    private String south;
    private String north;
    private Path inputURL;
    /**File system for the output directory*/
    private FileSystem outFS;
    private Path outDir;
    /**Output format one of the values {"images", "kmz", "video"}*/
    private String output;
    /**How to recover missing values (none|read|write)*/
    private String recover;

    public ImageRequestHandler(HttpServletRequest request) throws IOException, ParseException {
      outFS = FileSystem.get(commonParams);
      do {
        this.outDir = new Path(String.format("%06d", (int)(Math.random() * 1000000)));
      } while (outFS.exists(outDir));
      this.requesterName = request.getParameter("user_name");
      this.email = request.getParameter("email");
      this.datasetPath = request.getParameter("dataset_url");
      this.datasetName = request.getParameter("dataset");
      this.west = request.getParameter("min_lon");
      this.east = request.getParameter("max_lon");
      this.south = request.getParameter("min_lat");
      this.north = request.getParameter("max_lat");
      this.output = request.getParameter("output");
      this.recover = request.getParameter("recover");

      final SimpleDateFormat inputDateFormat = new SimpleDateFormat("MM/dd/yyyy");
      startDate = inputDateFormat.parse(request.getParameter("fromDate")).getTime();
      endDate = inputDateFormat.parse(request.getParameter("toDate")).getTime();
      // Limit time to thirty days
      endDate = Math.min(endDate, startDate + 30L * 24 * 60 * 60 * 1000);

      // Create the query parameters
      this.rect = west+','+south+','+east+','+north;
    }
    
    @Override
    public void run() {
      try {
        sendConfirmEmail();
        boolean imageSuccess = generateImage();
        if (!imageSuccess) {
          sendFailureEmail(null);
        } else {
          sendSuccessEmail();
        }
      } catch (Exception e) {
        e.printStackTrace();
        try {
          sendFailureEmail(e);
        } catch (AddressException e1) {
          e1.printStackTrace();
        } catch (UnsupportedEncodingException e1) {
          e1.printStackTrace();
        } catch (MessagingException e1) {
          e1.printStackTrace();
        }
      } finally {
        try {
          outFS.delete(outDir, true);
        } catch (IOException e) {
          LOG.warn("Error cleaning up the intermediate data");
        }
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
      
      Session mailSession = Session.getInstance(props,
          new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, password);
        }
      });
      
      Message message = new MimeMessage(mailSession);
      message.setFrom(new InternetAddress(from, "SHAHED Team"));
      InternetAddress requesterAddress = new InternetAddress(email, requesterName);
      message.addRecipient(RecipientType.TO, requesterAddress);
      InternetAddress adminAddress = new InternetAddress("eldawy@cs.umn.edu", "Ahmed Eldawy");
      message.addRecipient(RecipientType.BCC, adminAddress);
      message.setSubject("Confirmation: Your request was received");
      final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
      message.setText("Dear "+requesterName+",\n"+
          "Your request was received. "+
          "The server is currently processing your request and you will receive " +
          "an email with the generated files as soon as the request is complete.\n\n"+
          "Request parameters:\n"+
          String.format(" region: {south: %s, west: %s, north: %s, east:%s}", south, west, north, east)+"\n"+
          " Start date: "+dateFormat.format(startDate)+"\n"+
          " end date: "+dateFormat.format(endDate)+"\n"+
          "Thank you for using Shahed. \n\n Shahed team");
      message.setReplyTo(new InternetAddress[] {new InternetAddress(from, "SHAHED Team")});
      
      Transport.send(message, message.getAllRecipients());
      LOG.info("Message sent successfully to '"+requesterAddress+"'");
    }

    /**
     * Plots the image as the user requested
     * @return - whether the image generation was successful or not
     * @throws IOException
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     * @throws ParseException 
     */
    private boolean generateImage() throws IOException, InterruptedException, ClassNotFoundException, ParseException {
      this.inputURL = new Path(dataPath, datasetPath);
      // Launch the MapReduce job that plots the dataset
      OperationsParams plotParams = new OperationsParams(commonParams);
      plotParams.setBoolean("vflip", true);
      plotParams.setClass("shape", NASARectangle.class, Shape.class);
      plotParams.set("rect", rect);
      if (recover != null)
        plotParams.set("recover", recover);
      plotParams.set("dataset", datasetName);
      final SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy.MM.dd");
      plotParams.set("time", outputDateFormat.format(startDate)+".."+outputDateFormat.format(endDate));
      plotParams.setBoolean("background", true);
      
      return MultiHDFPlot.multiplot(new Path[] {inputURL}, outDir, plotParams);
    }

    /**
     * Sends an email to the client with the generated image uon success.
     * @param kmlBytes
     * @throws MessagingException 
     * @throws AddressException 
     * @throws IOException 
     */
    private void sendSuccessEmail() throws AddressException, MessagingException, IOException {
      Properties props = new Properties(MAIL_PROPERTIES);
      
      Session mailSession = Session.getInstance(props,
          new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, password);
        }
      });
      
      Message message = new MimeMessage(mailSession);
      message.setFrom(new InternetAddress(from, "SHAHED Team"));
      String toLine = requesterName+'<'+email+'>';
      message.setRecipients(RecipientType.TO, InternetAddress.parse(toLine));
      InternetAddress adminAddress = new InternetAddress("eldawy@cs.umn.edu", "Ahmed Eldawy");
      message.addRecipient(RecipientType.BCC, adminAddress);
      message.setSubject("Your request is complete");
      
      Multipart multipart = new MimeMultipart();
      
      MimeBodyPart textPart = new MimeBodyPart();
      textPart.setText("Dear "+requesterName+",\n"+
          "Your request was successfully completed. "+
          "Please find the generated images attached.\n\n"+
          "Thank you for using Shahed. \n\n Shahed team");
      multipart.addBodyPart(textPart);
      
      FileSystem outFS = outDir.getFileSystem(commonParams);
      FileStatus[] generatedFiles = outFS.listStatus(outDir);
      
      // Create a KMZ file and attach it to the email
      ByteArrayOutputStream kmzFile = new ByteArrayOutputStream();
      ZipOutputStream zipOut = new ZipOutputStream(kmzFile);
      byte[] buffer = new byte[1024*1024];
      for (FileStatus generatedFile : generatedFiles) {
        ZipEntry ze = new ZipEntry(generatedFile.getPath().getName());
        zipOut.putNextEntry(ze);
        FSDataInputStream in = outFS.open(generatedFile.getPath());
        int size;
        while ((size = in.read(buffer)) > 0) {
          zipOut.write(buffer, 0, size);
        }
        in.close();
        zipOut.closeEntry();
      }
      zipOut.close();
      byte[] kmzBytes = kmzFile.toByteArray();

      MimeBodyPart kmzPart = new MimeBodyPart();
      DataSource source2 = new ByteArrayDataSource(kmzBytes, "application/vnd.google-earth.kmz");
      kmzPart.setDataHandler(new DataHandler(source2));
      kmzPart.setFileName("heatmap.kmz");
      multipart.addBodyPart(kmzPart);
      
      message.setContent(multipart);
      Transport.send(message, message.getAllRecipients());
      
      LOG.info("Request finished successfully");
    }
    
    private void sendFailureEmail(Exception e) throws AddressException, MessagingException, UnsupportedEncodingException {
      Properties props = new Properties(MAIL_PROPERTIES);

      Session mailSession = Session.getInstance(props,
          new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, password);
        }
      });
      
      Message message = new MimeMessage(mailSession);
      InternetAddress requesterAddress = new InternetAddress(email, requesterName);
      message.setFrom(new InternetAddress(from, "SHAHED Team"));
      message.addRecipient(RecipientType.TO, requesterAddress);
      InternetAddress adminAddress = new InternetAddress("eldawy@cs.umn.edu", "Ahmed Eldawy");
      message.addRecipient(RecipientType.BCC, adminAddress);
      message.setSubject("Confirmation: Your request has failed");
      message.setText("Dear "+requesterName+",\n"+
          "Unfortunately there was an internal error while processing your request.\n"+
          e.getMessage() + "\n" +
          "Sorry for inconvenience. \n\n Shahed team");
      message.setReplyTo(new InternetAddress[] {new InternetAddress(from, "SHAHED Team")});
      
      Transport.send(message, message.getAllRecipients());
      LOG.info("Message sent successfully to '"+requesterAddress+"'");
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
    System.out.println("from:<email> - (*) Email to send from");
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
    if (!params.checkInput()) {
      System.err.println("Please specify the path of the indexed data");
      printUsage();
      System.exit(1);
    }
    Path[] paths = params.getPaths();
    Path datasetPath = paths.length == 1? new Path("http://e4ftl01.cr.usgs.gov/") : paths[0];
    Path indexPath = paths.length == 1? paths[0] : paths[1];
    startServer(datasetPath, indexPath, params);
  }
}
