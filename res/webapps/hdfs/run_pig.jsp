<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.io.LongWritable"
  import="org.apache.hadoop.io.Text"
  import="java.io.File"
  import="java.io.FilenameFilter"
  import="java.io.PrintStream"
  import="edu.umn.cs.spatialHadoop.core.SpatialSite"
  import="java.io.BufferedReader"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="edu.umn.cs.spatialHadoop.util.JspSpatialHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.mapred.LineRecordReader"
  import="org.apache.hadoop.mapred.FileSplit"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  String pigeonTempDir = conf.get("pigeon.tmp", ".");
  
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
    response.sendError(500, "Unable to create directory '"+newScriptDir+"'");
  } else {
    // Extract script name and body from request parameters
    String scriptBody = HtmlQuoting.unquoteHtmlChars(request.getParameter("script-body"));
    String scriptName = HtmlQuoting.unquoteHtmlChars(request.getParameter("script-name"));

    // Start the script in the given directory
    File scriptFile = new File(newScriptDir, "script.pig");
    PrintStream ps = new PrintStream(scriptFile);
    ps.println(scriptBody);
    ps.close();
    
    Text stdout = new Text();
    Text stderr = new Text();
    
    int exitCode = JspSpatialHelper.runProcess(newScriptDir, "pig -c -f script.pig", null, stdout, stderr, true);
    if (exitCode == 0) {
      // Run the script for real
      JspSpatialHelper.runProcess(newScriptDir, "pig -f script.pig -l script.log", null, null, null, false);
      
      // Write meta data to a file
      File metafileName = new File(newScriptDir, "metadata");
      ps = new PrintStream(metafileName);
      ps.println("scriptName: "+scriptName);
      ps.close();
      
      out.println("Script submit successfully");
      out.println("ID #"+scriptId);
    } else {
      // No need to keep the directory for scripts with syntax errors
      for (String file : newScriptDir.list()) {
        new File(newScriptDir, file).delete();
      }
      newScriptDir.delete();

      response.sendError(500, stderr.toString());
    }
  }
  
%>