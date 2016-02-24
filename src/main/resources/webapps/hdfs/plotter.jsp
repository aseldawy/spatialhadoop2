<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.*"
  import="java.awt.image.BufferedImage"
  import="edu.umn.cs.spatialHadoop.operations.GeometricPlot"
  import="edu.umn.cs.spatialHadoop.core.SpatialSite"
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

<%! private BufferedImage mergePhotos(Path[] imageFiles) {
  return null;
}
%>

<%
  Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  String files = request.getParameter("files");
  if (files != null) {
    String[] filenames = HtmlQuoting.unquoteHtmlChars(files).split(",");
    Path[] filepaths = new Path[filenames.length];
    boolean ready = true;
    for (int i = 0; ready && i < filenames.length; i++) {
      filepaths[i] = new Path(filenames[i]);
      FileSystem fs = filepaths[i].getFileSystem(conf);
      if (SpatialSite.getGlobalIndex(fs, filepaths[i]) == null ||
        !fs.exists(new Path(filepaths[i], "_data.png"))) {
        out.println("File '" + filenames[i] + "' is not ready for plot<br/>");
        out.println("<form action='/preprocess.jsp'>");
        out.println("<input type='hidden' name='path' value='"+filepaths[i]+"'/>");
        out.println("<span class='label'><label for='color'>Plot Color</label></span>");
        out.println("<select name='color'>");
        String[] colors = "black,blue,cyan,green,red,pink,orange".split(",");
        for (String color : colors) {
          out.println("<option>"+color+"</option>");
        }
        out.println("</select><br/>");
        out.println("<input type='Submit' value='Preprocess'/>");
        out.println("</form>");
        ready = false;
      }
    }
    if (ready) {
      try {
        boolean showPartitions = request.getParameter("partitions") != null &&
            request.getParameter("partitions").equals("true");
        BufferedImage combinedImage = GeometricPlot.combineImages(conf, filepaths,
          showPartitions, 1000, 1000);
        
        // Encode the image in base64
        ByteArrayOutputStream imageOut = new ByteArrayOutputStream();
        ImageIO.write(combinedImage, "png", imageOut);
        imageOut.close();
        
        byte[] imageData = imageOut.toByteArray();
        byte[] imageEncoded = Base64.encodeBase64(imageData);
        out.print("<img src=\"data:image/png;base64,");
        out.print(new String(imageEncoded));
        out.println("\"/>");
      } catch (Exception e) {
        out.println(e);
        out.println(Arrays.asList(e.getStackTrace()));
      }
    }
  }
  
%>
