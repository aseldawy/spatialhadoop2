<%@ page
  contentType="text/html; charset=UTF-8"
  import="edu.umn.cs.spatialHadoop.operations.RangeQuery"
  import="edu.umn.cs.spatialHadoop.OperationsParams"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="edu.umn.cs.spatialHadoop.core.*"
  import="edu.umn.cs.spatialHadoop.osm.*"
  import="org.apache.hadoop.mapreduce.Job"
  
  
  import="java.io.*"
  import="java.net.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.net.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"

%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  if (request.getParameter("input") == null ||
      request.getParameter("xmin") == null ||
      request.getParameter("ymin") == null ||
      request.getParameter("xmax") == null ||
      request.getParameter("ymax") == null) {
    out.println("Missing input or query arguments");
  } else {
    Path input = new Path(request.getParameter("input"));
    double xmin = Double.parseDouble(request.getParameter("xmin"));
    double ymin = Double.parseDouble(request.getParameter("ymin"));
    double xmax = Double.parseDouble(request.getParameter("xmax"));
    double ymax = Double.parseDouble(request.getParameter("ymax"));
    Rectangle query_mbr = new Rectangle(xmin, ymin, xmax, ymax);
    Path output = new Path(request.getParameter("output"));
    
    Configuration conf =
      (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    
    try{
      OperationsParams params = new OperationsParams(conf);
      OperationsParams.setShape(params, "rect", query_mbr);
      params.setBoolean("background", true);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      Job running_job = RangeQuery.rangeQueryMapReduce(input, output, params);
      
      // Create a link to the status of the running job
      String trackerAddress = conf.get("mapred.job.tracker.http.address");
      InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(trackerAddress);
      String requestUrl = request.getRequestURL().toString();
      int cutoff = requestUrl.indexOf('/', requestUrl.lastIndexOf(':'));
      requestUrl = requestUrl.substring(0, cutoff);
      InetSocketAddress requestSocAddr = NetUtils.createSocketAddr(requestUrl);
      if (running_job != null) {
        out.println("Job #"+running_job.getJobID()+" submitted successfully<br/>");
        out.print("<a target='_blank' href='"+
          "http://"+requestSocAddr.getHostName()+":"+infoSocAddr.getPort()+
          "/jobdetails.jsp?jobid="+running_job.getJobID()+"&amp;refresh=30"+
          "'>");
        out.print("Click here to track the job");
        out.println("</a>");
      } else {
        out.println("Job submitted successfully");
      }
    } catch(Exception e) {
      out.println(e);
      for (StackTraceElement ste : e.getStackTrace()) {
        out.println(ste);
        out.println("<br/>");
      }
    }
  }
%>

Done