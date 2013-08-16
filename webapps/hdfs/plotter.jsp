<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.fs.Path"
  import="java.awt.image.BufferedImage"
  import="edu.umn.cs.spatialHadoop.operations.Plot"
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

<h1>
  <%
    Configuration conf = (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    String files = request.getParameter("files");
    if (files != null) {
      String[] filenames = HtmlQuoting.unquoteHtmlChars(files).split(",");
      Path[] filepaths = new Path[filenames.length];
      for (int i = 0; i < filenames.length; i++)
        filepaths[i] = new Path(filenames[i]);
      try {
        BufferedImage combinedImage = Plot.combineImages(conf, filepaths, false, 1000, 1000);
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
    
  %>
</h1>
