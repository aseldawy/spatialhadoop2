<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="java.io.*"
  import="java.awt.Color"
  import="org.apache.hadoop.io.Text"
  import="java.awt.image.BufferedImage"
  import="edu.umn.cs.spatialHadoop.operations.*"
  import="edu.umn.cs.spatialHadoop.core.*"
  import="java.io.BufferedReader"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.hdfs.server.namenode.JspHelper"
  import="org.apache.hadoop.conf.Configuration"
  import="java.util.Arrays"
  import="java.io.ByteArrayOutputStream"
  import="javax.imageio.ImageIO"
  import="org.apache.commons.codec.binary.Base64"
%>

<%! private static final long serialVersionUID = 1L;%>
<%! static JspHelper jspHelper = new JspHelper(); %>

<%
  final Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  if (request.getParameter("path") != null) {
    final Path path = new Path(request.getParameter("path"));
    final FileSystem fs = path.getFileSystem(conf);
    final javax.servlet.jsp.JspWriter jsp_out = out;
    
    // 1- Check if index file is there
    if (SpatialSite.getGlobalIndex(fs, path) == null) {
      // Write an index file
      new Thread(new Runnable() {
        public void run() {
          try {
            Rectangle mbr = FileMBR.fileMBR(fs, path, new OSMPolygon());
            FileStatus[] datafiles = fs.listStatus(path,new PathFilter(){
              public boolean accept(Path p){
                String name = p.getName(); 
                return !name.startsWith("_") && !name.startsWith("."); 
              }
            });
            Path gindex_path = new Path(path, "_master.grid");
            PrintStream gout = new PrintStream(fs.create(gindex_path, false));
            for (FileStatus datafile : datafiles) {
              gout.print(mbr.toText(new Text()));
              gout.print(",");
              gout.print(datafile.getPath().getName());
              gout.println();
            }
            gout.close();
          } catch (IOException e) {
            //jsp_out.println(e);
            //for (StackTraceElement ste : e.getStackTrace()) {
            //  jsp_out.println(ste);
            //  jsp_out.println("<br/>");
            //}
          }
        }
      }).start();
    }
    
    // 2- Check if the plotted image is there
    if (!fs.exists(new Path(path, "_data.png"))) {
      // Plot the image
      Color color = Color.BLACK;
      String colorName = request.getParameter("color");
      if (colorName != null) {
        colorName = colorName.toLowerCase();
        if (colorName.equals("red")) {
          color = Color.RED;
        } else if (colorName.equals("pink")){
          color = Color.PINK;
        } else if (colorName.equals("blue")){
          color = Color.BLUE;
        } else if (colorName.equals("green")) {
          color = Color.GREEN;
        } else if (colorName.equals("black")) {
          color = Color.BLACK;
        } else if (colorName.equals("orange")) {
          color = Color.ORANGE;
        } else if (colorName.equals("cyan")) {
          color = Color.CYAN;
        }
      }
      
      Plot.plot(path, new Path(path, "_data.png"), new OSMPolygon(),
        1000, 1000, color, false, false, false);
    }
  }
%>

Done!