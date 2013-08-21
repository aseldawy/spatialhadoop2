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
    out.println("<option value='"+fstatus.getPath()+"'>"+fstatus.getPath().getName()+"</option>");
  }
}
%>

<html>
<head>
<link rel="stylesheet" href="/static/visualizer/visualizer.css"/>
<script language="javascript" src="/static/visualizer/jquery-1.10.2.min.js"></script>
<script language="javascript" src="/static/visualizer/visualizer.js"></script>
<title>Spatial Hadoop Visualizer</title>
</head>
<body>
<div id="filenames" style="float: left;">
  <img width="100" src="/static/visualizer/shadoop_logo.png"/>
  <div class="toolbar" style="width: 100%;">
    <div style="background: url('/static/visualizer/add.png')" title="Add file"></div>
    <div style="background: url('/static/visualizer/delete.png')" title="Delete file"></div>
  </div>
  <select id="file-selector" name="filename" multiple="multiple" style="height: 300px; width:120px;">
    <% String dir = HtmlQuoting.unquoteHtmlChars(request.getParameter("dir")); %>
    <% Path path = dir == null? new Path("/") : new Path(dir); %>
    <% listDirectory(request, out, path); %>
 </select>
</div>
<div id="image" style="float: left;">
  <image height="80"/>
  <div class="operations" style="width: 100%;">
    <input type="button" value="Range Query"></input>
    <input type="button" value="kNN"></input>
    <input type="button" value="Spatial Join" id="spatial-join-button"></input>
  </div>
  <div id="preview-img">
    Preview place holder
  </div>
</div>
<!-- Spatial join dialog -->
<div class="dialog" id="spatial-join-dialog">
  <div class="title">Spatial Join</div>
  <form action="/spatialjoin.jsp">
    <p><span class="label"><label for="input1">Input filename</label></span>
    <input name="input1" type="text"/></p>
    <p><span class="label"><label for="input2">Input filename</label></span>
    <input name="input2" type="text"/></p>
    <p><span class="label"><label for="predicate">Join predicate</label></span>
    <select name="predicate">
      <option>overlap</option>
    </select></p>
    <p>
    <p><span class="label"><label for="output">Output filename</label></span>
    <input name="output" type="text"/></p>
    <textarea cols="40" rows="8">
area_water = LOAD 'area_water' AS (id: int, area: polygon);
road_edges = LOAD 'road_edges' AS (id: int, edge: line);
result     = JOIN area_water BY area
  road_edges BY edge PREDICATE = overlap;
STORE result INTO '/roads_rivers';
    </textarea>
    </p>
    <p>
    <input type="submit" value="Submit"/>
    <input type="reset" value="Cancel"/>
    </p>
  </form>
</div>
<!-- end of spatial join dialog -->
</body>
</html>