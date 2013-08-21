<%@ page
  contentType="text/html; charset=UTF-8"
  import="edu.umn.cs.spatialHadoop.operations.DistributedJoin"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="edu.umn.cs.spatialHadoop.core.OSMPolygon"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  Path input1 = new Path(request.getParameter("input1"));
  Path input2 = new Path(request.getParameter("input2"));
  String predicate = request.getParameter("predicate");
  Path output = new Path(request.getParameter("output"));
  
  Configuration conf =
    (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  
  try{
  DistributedJoin.joinStep(input1.getFileSystem(conf), new Path[] {input1, input2},
    output, new OSMPolygon(), false);
    } catch(Exception e) {
      out.println(e);
      e.printStackTrace(out);
    }
%>

Done