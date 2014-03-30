<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.io.LongWritable"
  import="org.apache.hadoop.io.Text"
  import="java.io.File"
  import="java.io.FileInputStream"
  import="java.io.InputStream"
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
  String strScriptId = request.getParameter("id");
  String part = request.getParameter("part");
  if (strScriptId == null) {
    response.sendError(500, "Parameter 'id' missing");
  } else {
    int scriptId = Integer.parseInt(strScriptId);
    File scriptDir = new File(pigeonTempDir, String.format("pigeon_%04d", scriptId));
    if (!scriptDir.isDirectory()) {
      response.sendError(404, "Script #"+scriptId+" does not exist in path '"+scriptDir+"'");
    } else {
      if (part.equals("body")) {
        // Retrieve script body (Pig script)
        File scriptBodyFile = new File(scriptDir, "script.pig");
        byte[] scriptBodyBytes = new byte[(int)scriptBodyFile.length()];
        InputStream in = new FileInputStream(scriptBodyFile);
        in.read(scriptBodyBytes);
        in.close();
        
        out.print(new String(scriptBodyBytes));
      } else if (part.equals("log")) {
        // Retrieve script progress from log
        String[] logFile = scriptDir.list(new FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.toLowerCase().endsWith("log");
          }
        });
        if (logFile.length > 0) {
          File scriptLogFile = new File(scriptDir, logFile[0]);
          byte[] scriptLogBytes = new byte[(int)scriptLogFile.length()];
          InputStream in = new FileInputStream(scriptLogFile);
          in.read(scriptLogBytes);
          in.close();
          
          out.print(new String(scriptLogBytes));
        } else {
          response.sendError(404, "Log file not found");
        }
      }
    }
  }
  
%>