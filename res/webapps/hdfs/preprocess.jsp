<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="java.io.*"
  import="java.awt.Color"
  import="org.apache.hadoop.io.Text"
  import="java.awt.image.BufferedImage"
  import="edu.umn.cs.spatialHadoop.operations.*"
  import="edu.umn.cs.spatialHadoop.core.*"
  import="java.io.BufferedReader"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="edu.umn.cs.spatialHadoop.util.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="java.util.Arrays"
  import="java.io.ByteArrayOutputStream"
  import="javax.imageio.ImageIO"
  import="org.apache.commons.codec.binary.Base64"
  import="org.apache.hadoop.mapred.RunningJob"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  final Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  if (request.getParameter("path") != null) {
    final Path path = new Path(request.getParameter("path"));
    final FileSystem fs = path.getFileSystem(conf);
    final javax.servlet.jsp.JspWriter jsp_out = out;
    
    // 1- Check if index file is there
    if (SpatialSite.getGlobalIndex(fs, path) == null) {
      // Compute the MBR and store it back to disk
      FileMBR.fileMBRMapReduce(fs, path, new OSMPolygon(), true);
      RunningJob job = FileMBR.lastSubmittedJob;
      String jobUrl =
        JspHelper.jobTrackUrl(request.getRequestURL().toString(), conf, job);
      out.println("MBR job submitted<br/>");
      out.println("<a href='"+jobUrl+"'>Track Job #"+job.getID()+" here</a><br/>");
    }
    
    // 2- Check if the plotted image is there
    if (!fs.exists(new Path(path, "_data.png"))) {
      // Plot the image
      Color color = Color.BLACK;
      String colorName = request.getParameter("color");
      if (colorName != null) {
        colorName = colorName.toLowerCase();
        if (colorName.equals("red")) {
          color = Color.RED;
        } else if (colorName.equals("pink")){
          color = Color.PINK;
        } else if (colorName.equals("blue")){
          color = Color.BLUE;
        } else if (colorName.equals("green")) {
          color = Color.GREEN;
        } else if (colorName.equals("black")) {
          color = Color.BLACK;
        } else if (colorName.equals("orange")) {
          color = Color.ORANGE;
        } else if (colorName.equals("cyan")) {
          color = Color.CYAN;
        }
      }
      
      Plot.plotMapReduce(path, new Path(path, "_data.png"), new OSMPolygon(),
        1000, 1000, color, false, false, false, true);
      RunningJob job = Plot.lastSubmittedJob;
      String jobUrl =
        JspHelper.jobTrackUrl(request.getRequestURL().toString(), conf, job);
      out.println("Plot job submitted<br/>");
      out.println("<a href='"+jobUrl+"'>Track Job #"+job.getID()+" here</a><br/>");
    }
  }
%>

Done!