<%@ page
  contentType="text/html; charset=UTF-8"
  import="edu.umn.cs.spatialHadoop.operations.DistributedJoin"
  import="org.apache.hadoop.conf.Configuration"
  import="edu.umn.cs.spatialHadoop.OperationsParams"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="edu.umn.cs.spatialHadoop.core.OSMPolygon"
  import="edu.umn.cs.spatialHadoop.core.Shape"
  import="org.apache.hadoop.mapred.RunningJob"
  
  
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
  if (request.getParameter("input1") == null ||
      request.getParameter("input2") == null) {
    out.println("Missing input arguments");
  } else {
    Path input1 = new Path(request.getParameter("input1"));
    Path input2 = new Path(request.getParameter("input2"));
    String predicate = request.getParameter("predicate");
    Path output = new Path(request.getParameter("output"));
    
    Configuration conf =
      (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    
    try{
      OperationsParams params = new OperationsParams(conf);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setBoolean("background", true);
      DistributedJoin.joinStep(new Path[] {input1, input2}, output, params);
      RunningJob running_job = DistributedJoin.lastRunningJob;
      String trackerAddress = conf.get("mapred.job.tracker.http.address");
      InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(trackerAddress);
      String requestUrl = request.getRequestURL().toString();
      int cutoff = requestUrl.indexOf('/', requestUrl.lastIndexOf(':'));
      requestUrl = requestUrl.substring(0, cutoff);
      InetSocketAddress requestSocAddr = NetUtils.createSocketAddr(requestUrl);
      out.println("Job #"+running_job.getID()+" submitted successfully<br/>");
      out.print("<a target='_blank' href='"+
        "http://"+requestSocAddr.getHostName()+":"+infoSocAddr.getPort()+
        "/jobdetails.jsp?jobid="+running_job.getID()+"&amp;refresh=30"+
        "'>");
      out.print("Click here to track the job");
      out.println("</a>");
    } catch(Exception e) {
      e.printStackTrace(out);
    }
  }
%>

Done