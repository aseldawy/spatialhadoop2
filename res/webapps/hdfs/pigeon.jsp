<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.util.Vector"
  import="java.io.BufferedReader"
  import="java.io.FileReader"
  import="java.io.File"
  import="java.io.FilenameFilter"
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

private void listPigeonScripts(Configuration conf, Vector<Integer> ids,
  Vector<String> names) throws java.io.IOException {
  File pigeonTempDir = new File(conf.get("pigeon.tmp", "."));
  
  // Return directories of all previous attempts
  String[] previousScripts = pigeonTempDir.list(new FilenameFilter() {
    public boolean accept(File dir, String name) {
      return name.matches("pigeon_[0-9]+");
    }
  });
  
  for (String previousScript : previousScripts) {
    int id = Integer.parseInt(previousScript.replace("pigeon_", ""));
    File metadataFile = new File (new File(pigeonTempDir, previousScript), "metadata");
    BufferedReader reader = new BufferedReader(new FileReader(metadataFile));
    String line;
    String scriptName = null;
    while (scriptName == null && (line = reader.readLine()) != null) {
      if (line.startsWith("scriptName:"))
        scriptName = line.replace("scriptName:", "").trim();
    }
    reader.close();
    
    ids.add(id);
    names.add(scriptName);
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
  <img width="100" src="/static/pigeon/pigeon_logo.png"/>
  <h3>Relations</h3>
  <select id="file-selector" name="filename" multiple="multiple" style="height: 150px; width:100%;">
    <% String dir = HtmlQuoting.unquoteHtmlChars(request.getParameter("dir")); %>
    <% Path path = dir == null? new Path("/") : new Path(dir); %>
    <% listDirectory(request, out, path); %>
 </select>

<h3>Queries</h3>
<select multiple="multiple" style="width: 100%;">
  <%
    Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    Vector<Integer> scriptIds = new Vector<Integer>();
    Vector<String> scriptNames = new Vector<String>();
    listPigeonScripts(conf, scriptIds, scriptNames);
    
    for (int i = 0; i < scriptIds.size(); i++) {
    
      out.println("<option value='"+scriptIds.get(i)+"'>"+scriptNames.get(i)+"</option>");
    }
  %>
</select>

</div>

<div style="float: left; width: 800px;">
  <h3>Preview area</h3>
  <div style="width: 100%; height: 150px; border: 1px solid black; overflow: auto;">
  <pre id="preview-head"> </pre>
  </div>
  
  <h3>Pigeon script</h3>
  <form id="run-pig" method="post" action="run_pig.jsp">
    <div style="width: 100%; height: 300px;">
      <textarea name="script-body" style="width: 100%; height: 280px;">
      </textarea>
    <label for="script-name">Query Name</label>
    <input type="text" name="script-name" id="script-name" value="" placeholder="Script name"/>
    <input type="submit" value="Submit Query"/>
  </form>
  </div>
</div>

</body>
</html>