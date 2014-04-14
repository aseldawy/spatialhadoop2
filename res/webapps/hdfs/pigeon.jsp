<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.util.Vector"
  import="java.util.Arrays"
  import="java.io.BufferedReader"
  import="java.io.FileReader"
  import="java.io.File"
  import="java.io.FilenameFilter"
  import="java.io.PrintStream"
  import="java.io.IOException"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.security.UserGroupInformation"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.server.namenode.FSNamesystem"
  import="org.apache.hadoop.hdfs.server.datanode.DataNode"
  import="org.apache.hadoop.io.Text"
  import="edu.umn.cs.spatialHadoop.util.JspSpatialHelper"
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
  
  Arrays.sort(previousScripts);
  
  for (String previousScript : previousScripts) {
    int id = Integer.parseInt(previousScript.replace("pigeon_", ""));
    File metadataFile = new File (new File(pigeonTempDir, previousScript), "metadata");
    if (metadataFile.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(metadataFile));
      String line;
      String scriptName = null;
      while (scriptName == null && (line = reader.readLine()) != null) {
        if (line.startsWith("scriptName:"))
          scriptName = line.replace("scriptName:", "").trim();
      }
      reader.close();
      
      ids.insertElementAt(id, 0);
      names.insertElementAt(scriptName, 0);
    }
  }
}

private String runPigeonScript(Configuration conf, String scriptName, String scriptBody) {
  try {
  String pigeonTempDir = conf.get("pigeon.tmp", ".");
  String pigHome = conf.get("pig.home");
  File pigExecutable = pigHome == null? new File("pig") : new File(new File(pigHome, "bin"), "pig");
  
  // Return directories of all previous attempts
  String[] previousScripts = new File(pigeonTempDir).list(new FilenameFilter() {
    public boolean accept(File dir, String name) {
      return name.matches("pigeon_[0-9]+");
    }
  });

  // Find the maximum job ID and generate a new ID by adding one
  int maxId = 0;
  for (String previousScript : previousScripts) {
    int id = Integer.parseInt(previousScript.replace("pigeon_", ""));
    if (id > maxId)
      maxId = id;
  }
  int scriptId = maxId + 1;
  
  File newScriptDir = new File(pigeonTempDir, String.format("pigeon_%04d", scriptId));
  if (!newScriptDir.mkdirs()) {
    return "Unable to create directory '"+newScriptDir+"'";
  } else {
    // Start the script in the given directory
    File scriptFile = new File(newScriptDir, "script.pig");
    PrintStream ps = new PrintStream(scriptFile);
    ps.println(scriptBody);
    ps.close();
    
    Text stdout = new Text();
    Text stderr = new Text();
    int exitCode = JspSpatialHelper.runProcess(newScriptDir, pigExecutable+" -c -f script.pig", null, stdout, stderr, true);
    if (exitCode == 0) {
      // Run the script for real
      JspSpatialHelper.runProcess(newScriptDir, pigExecutable+" -f script.pig -l script.log", null, null, null, false);
      
      // Write meta data to a file
      File metafileName = new File(newScriptDir, "metadata");
      ps = new PrintStream(metafileName);
      ps.println("scriptName: "+scriptName);
      ps.close();
      return null;
    } else {
      // No need to keep the directory for scripts with syntax errors
      for (String file : newScriptDir.list()) {
        new File(newScriptDir, file).delete();
      }
      newScriptDir.delete();

      return stderr.toString();
    }
  }
  } catch (IOException e) {
    e.printStackTrace();
    return "error: "+e;
  }
}
%>

<%
  String scriptStatus = null;
  if (request.getParameter("script-body") != null) {
    Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    
    // Extract script name and body from request parameters
    String scriptBody = HtmlQuoting.unquoteHtmlChars(request.getParameter("script-body"));
    String scriptName = HtmlQuoting.unquoteHtmlChars(request.getParameter("script-name"));
    
    scriptStatus = runPigeonScript(conf, scriptName, scriptBody);
    // Indicates that a script was run on page load. So, we do not overwrite
    // the value in the #preview-head as it contains the result of running the
    // script.
    out.println("<script lanugage='javascript'>var run_script=true</script>");
  } else {
    out.println("<script lanugage='javascript'>var run_script=false</script>");
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
  <h2>Pigeon</h2>
  <h3>Relations</h3>
  <select id="file-selector" name="filename" size="8" style="height: 150px; width:100%;">
    <% String dir = HtmlQuoting.unquoteHtmlChars(request.getParameter("dir")); %>
    <% Path path = dir == null? new Path(".") : new Path(dir); %>
    <% listDirectory(request, out, path); %>
 </select>

<h3>Queries</h3>
<select size="10" id="pigeon-scripts" style="width: 100%;">
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
  <pre id="preview-head"><%= ""+scriptStatus %> </pre>
  </div>
  
  <h3>Pigeon script</h3>
  <form id="run-pig" method="post" action="pigeon.jsp">
    <div style="width: 100%; height: 300px;">
      <textarea name="script-body" id="script-body" style="width: 100%; height: 280px;">
<%= request.getParameter("script-body") == null ? "" : HtmlQuoting.unquoteHtmlChars(request.getParameter("script-body")) %>
      </textarea>
    <label for="script-name">Query Name</label>
    <input type="text" name="script-name" id="script-name" value="" placeholder="Script name"/>
    <input type="submit" value="Submit Query"/>
  </form>
  </div>
</div>

</body>
</html>