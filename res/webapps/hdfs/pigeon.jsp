<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.security.UserGroupInformation"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.server.namenode.FSNamesystem"
  import="org.apache.hadoop.hdfs.server.datanode.DataNode"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%!
private void listDirectory(HttpServletRequest request, JspWriter out,
    Path path) throws java.io.IOException {
  Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  final FileSystem fs = path.getFileSystem(conf);
  FileStatus[] fss = fs.listStatus(path);
  for (FileStatus fstatus : fss) {
    out.println("<option value='"+fstatus.getPath().toUri().getPath()+"'>"+fstatus.getPath().getName()+"</option>");
  }
}
%>

<html>
<head>
<!--link rel="stylesheet" href="/static/visualizer/visualizer.css"/-->
<script language="javascript" src="/static/visualizer/jquery-1.10.2.min.js"></script>
<script language="javascript" src="/static/pigeon/pigeon.js"></script>
<title>Pigeon Demonstration</title>
</head>
<body>
<div id="filenames" style="float: left; width: 150px;">
  <img width="100" src="/static/visualizer/pigeon_logo.png"/>
  <h3>Relations</h3>
  <select id="file-selector" name="filename" multiple="multiple" style="height: 150px; width:100%;">
    <% String dir = HtmlQuoting.unquoteHtmlChars(request.getParameter("dir")); %>
    <% Path path = dir == null? new Path("/") : new Path(dir); %>
    <% listDirectory(request, out, path); %>
 </select>

<h3>Queries</h3>
<select multiple="multiple" style="width: 100%;">
  <option value="Lakes x Roads">Lakes x Roads</option>
  <option value="Lakes x Roads">Average City Area</option>
</select>

</div>

<div style="float: left; width: 800px;">
  <h3>Preview area</h3>
  <div style="width: 100%; height: 150px; border: 1px solid black; overflow: auto;">
  <pre id="preview-head"> </pre>
  </div>
  
  <h3>Pigeon script</h3>
  <form id="run-pig" action="run_pig.jsp">
    <div style="width: 100%; height: 300px;">
      <textarea name="script-body" style="width: 100%; height: 280px;">
      </textarea>
    <label for="script-name">Query Name</label>
    <input type="text" name="query-name" id="query-name" value="Extract nodes"/>
    <input type="submit" value="Submit Query"/>
  </form>
  </div>
</div>

</body>
</html>