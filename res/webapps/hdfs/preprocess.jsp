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
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="java.util.Arrays"
  import="java.io.ByteArrayOutputStream"
  import="javax.imageio.ImageIO"
  import="org.apache.commons.codec.binary.Base64"
  import="org.apache.hadoop.mapred.RunningJob"
  import="edu.umn.cs.spatialHadoop.util.JspSpatialHelper"
  import="edu.umn.cs.spatialHadoop.OperationsParams"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>
<%! static JspSpatialHelper jspSpatialHelper = new JspSpatialHelper(); %>

<%
  final Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  if (request.getParameter("path") != null) {
    final Path path = new Path(request.getParameter("path"));
    final FileSystem fs = path.getFileSystem(conf);
    final javax.servlet.jsp.JspWriter jsp_out = out;

    // Plot borders of the input relation if it is indexed
    if (!fs.exists(new Path(path, "_partitions.png"))) {
      // Retrieve the name of the master file
      FileStatus[] master = fs.listStatus(path, new PathFilter() {
        public boolean accept(Path p) {
          return p.getName().startsWith("_master");
        }
      });
      if (master.length != 0) {
        // Plot the image that shows partitions
        OperationsParams plotConf = new OperationsParams(conf);
        plotConf.setClass("shape", Partition.class, Shape.class);
        plotConf.set("color", "black");
        plotConf.setBoolean("background", true);
        plotConf.setBoolean("vflip", true);
        plotConf.setBoolean("local", true);
        
        Plot.plot(master[0].getPath(), new Path(path, "_partitions.png"), plotConf);
      }
    }
    // Check if the plotted image is there
    if (!fs.exists(new Path(path, "_data.png"))) {
      // Plot the image
      String colorName = request.getParameter("color");
      
      OperationsParams plotConf = new OperationsParams(conf);
      plotConf.setClass("shape", OSMPolygon.class, Shape.class);
      if (colorName != null)
        plotConf.set("color", colorName);
      plotConf.setBoolean("background", true);
      plotConf.setBoolean("vflip", true);
      plotConf.setBoolean("fast", true);
      
      Plot.plot(path, new Path(path, "_data.png"), plotConf);
      RunningJob job = Plot.lastSubmittedJob;
      String jobUrl =
        JspSpatialHelper.jobTrackUrl(request.getRequestURL().toString(), conf, job);
      out.println("Plot job submitted<br/>");
      out.println("<a href='"+jobUrl+"'>Track Job #"+job.getID()+" here</a><br/>");
    }
  }
%>

Done!