/**
 * 
 */
package edu.umn.cs.spatialHadoop;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import edu.umn.cs.spatialHadoop.visualization.ImagePlot;


/**
 * @author sjais001
 *
 */
public class DavinciServer extends AbstractHandler {
	private static final Log LOG = LogFactory.getLog(DavinciServer.class);
	
	ImagePlot imagePlot;

	/**
	 * A constructor that starts the Jetty server
	 * @param dataPath
	 * @param indexPath
	 * @param params
	 */
	public DavinciServer() {
	}

	/**
	 * Create an HTTP web server (using Jetty) that will stay running to answer
	 * all queries
	 * @throws Exception 
	 */
	private static void startServer() throws Exception {
		int port = 8889;

		Server server = new Server(port);
		server.setHandler(new DavinciServer());
		server.start();
		server.join();
	}

	public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
			throws IOException, ServletException {
		// Bypass cross-site scripting (XSS)
		response.addHeader("Access-Control-Allow-Origin", "*");
		response.addHeader("Access-Control-Allow-Credentials", "true");
		((Request) request).setHandled(true);

		try {
			if (target.startsWith("/dynamic/showImage.cgi")) {
				displayImage(target, response);
			} else {
				LOG.info("Received request: '"+request.getRequestURL()+"'");
				tryToLoadStaticResource(target, response);
			}
		} catch (Exception e) {
			e.printStackTrace();
			//reportError(response, "Error placing the request", e);
		}
	}

	private void displayImage(String target, HttpServletResponse response) throws IOException {
		LOG.info("display image start time "+System.currentTimeMillis());
		target = target.replace("/dynamic/showImage.cgi/", "");
		
		if (new File(target).isFile() && target.endsWith("index.html")) {
			// The server requests the root HTML file
			tryToLoadStaticResource(target, response);
		} else if (new File(target).isFile() && target.endsWith(".png")) {
			// The server requests a pre-generated image (PNG file that exists on disk)
			double startTime = System.nanoTime();
			
			tryToLoadStaticResource(target, response);
			
			double finishTime = System.nanoTime();		
			LOG.info("#### STATIC file: "+target +"image load time is: "+(finishTime-startTime));
		} else if (target.endsWith(".png")) {
			String filename = new File(target).getName();
			if(filename.contains("--")){
				tryToLoadStaticResource(target, response);
			}
			else{
				String[] splits = filename.split("[\\-\\.]");
				int zoom_level = Integer.parseInt(splits[1]);
				int column = Integer.parseInt(splits[2]);
				int row = Integer.parseInt(splits[3]);
				Boolean upLevel = false;
				
				do {
					System.out.println("***************target: "+target);
					File datafile = new File(new File(target).getParent(), String.format("tile-%d-%d-%d.txt", zoom_level, column, row));
					if (datafile.exists()) {
						// There is a data file and we have to generate the image out of this data file
						// TODO read the data file and create the image correctly
						
						double startTime = System.nanoTime();
						String infilename =datafile.getName();
						
						imagePlot = new ImagePlot();
						DataOutputStream output = new DataOutputStream(response.getOutputStream());
						imagePlot.createImage(datafile.getParent(),infilename, output, upLevel, filename,zoom_level, column,row);
						upLevel = false;
						
						output.close();
						response.setContentType("image/png");
						response.setStatus(HttpServletResponse.SC_OK);
						
						double finishTime = System.nanoTime();
						LOG.info("****DATFILE : "+filename+" image generation and load time is: "+(finishTime-startTime));
						return;
					} else {
						// Go one level higher					
						zoom_level--;					
						column /= 2;
						row /= 2;
						upLevel = true;
					}
				} while (zoom_level >= 0);
			}
			// There is no such image, just return an empty image OR return a 404 NOT_FOUND response
			LOG.info("display image end time "+System.currentTimeMillis());
		} else {
			LOG.error("Cannot handle "+target);
		}
	}
	
	/**
	 * Tries to load the given resource name from class path if it exists.
	 * Used to serve static files such as HTML pages, images and JavaScript files.
	 * @param target
	 * @param response
	 * @throws IOException
	 */
	private void tryToLoadStaticResource(String target,
			HttpServletResponse response) throws IOException {
		LOG.info("Loading resource "+target);
		// Try to load this resource as a static page
		try {
			InputStream resource = new FileInputStream(target);
			byte[] buffer = new byte[1024*1024];
			ServletOutputStream outResponse = response.getOutputStream();
			int size;
			while ((size = resource.read(buffer)) != -1) {
				outResponse.write(buffer, 0, size);
			}
			resource.close();
			outResponse.close();
			response.setStatus(HttpServletResponse.SC_OK);
			if (target.endsWith(".js")) {
				response.setContentType("application/javascript");
			} else if (target.endsWith(".css")) {
				response.setContentType("text/css");
			} else {
				response.setContentType(URLConnection.guessContentTypeFromName(target));
			}
			final DateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ");
			final long year = 1000L * 60 * 60 * 24 * 365;
			// Expires in a year
			response.addHeader("Expires", format.format(new Date().getTime() + year));
		} catch (IOException e) {
			// File not found
			LOG.warn("File not found "+target);
			if (target.endsWith(".png")) {
				BufferedImage emptyImage = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB);
				Graphics2D graphics = emptyImage.createGraphics();
				/*graphics.setColor(Color.BLACK);
				graphics.drawLine(0, 0, 256, 256);
				graphics.drawLine(0, 256, 256, 0);
				graphics.dispose();*/
				ServletOutputStream outResponse = response.getOutputStream();
				ImageIO.write(emptyImage, "png", outResponse);
				outResponse.close();
				response.setContentType("image/png");
				response.setStatus(HttpServletResponse.SC_OK);
			}
			return;
		}
	}

	private void reportError(HttpServletResponse response, String msg,
			Exception e)
					throws IOException {
		if (e != null)
			e.printStackTrace();
		LOG.error(msg);
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		response.getWriter().println("{\"message\": '"+msg+"',");
		if (e != null) {
			response.getWriter().println("\"error\": '"+e.getMessage()+"',");
			response.getWriter().println("\"stacktrace\": [");
			for (StackTraceElement trc : e.getStackTrace()) {
				response.getWriter().println("'"+trc.toString()+"',");
			}
			response.getWriter().println("]");
		}
		response.getWriter().println("}");
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		startServer();
	}
//http://localhost:8889/dynamic/showImage.cgi/PRIMARYROADS.png/index.html
}
