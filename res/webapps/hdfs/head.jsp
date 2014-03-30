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
  String paramFile = request.getParameter("file");
  String paramLines = request.getParameter("lines");
  int numLines = paramLines == null? 10 : Integer.parseInt(paramLines);
  if (paramFile != null) {
    Path filePath = new Path(HtmlQuoting.unquoteHtmlChars(paramFile));
    FileSystem fs = filePath.getFileSystem(conf);
    if (fs.getFileStatus(filePath).isDir()) {
      // Directory selected, head one of the data files (any non hidden file)
      FileStatus[] dataFiles = fs.listStatus(filePath, SpatialSite.NonHiddenFileFilter);
      if (dataFiles.length == 0) {
        response.sendError(404, "No data files");
        filePath = null;
      } else {
        filePath = dataFiles[0].getPath();
      }
    }
    if (filePath != null) {
      LineRecordReader reader = new LineRecordReader(conf, new FileSplit(filePath, 0, 4096 * numLines, new String[0]));
      Text line = new Text();
      LongWritable offset = new LongWritable();
      while (numLines-- > 0 && reader.next(offset, line)) {
          out.println(line);
      }
      reader.close();
    }
  }
%>