<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.io.LongWritable"
  import="org.apache.hadoop.io.Text"
  import="edu.umn.cs.spatialHadoop.core.SpatialSite"
  import="java.io.BufferedReader"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.mapred.LineRecordReader"
  import="org.apache.hadoop.mapred.FileSplit"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  String scriptBody = request.getParameter("script");
  String scriptName = request.getParameter("name");
  
  Runtime.getRuntime.exec("");
%>