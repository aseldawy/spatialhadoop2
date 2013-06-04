package edu.umn.cs.spatialHadoop.operations;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.spatial.GridOutputFormat;
import org.apache.hadoop.mapred.spatial.ShapeInputFormat;
import org.apache.hadoop.mapred.spatial.ShapeRecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.JTSShape;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.SimpleGraphics;

public class Plot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(Plot.class);
  
  /**Whether or not to show partition borders (boundaries) in generated image*/
  private static final String ShowBorders = "plot.show_borders";
  private static final String ShowBlockCount = "plot.show_block_count";
  private static final String ShowRecordCount = "plot.show_record_count";
  private static final String CompactCells = "plot.compact_cells";

  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author Ahmed Eldawy
   *
   */
  public static class PlotMap extends MapReduceBase 
    implements Mapper<CellInfo, Shape, CellInfo, Shape> {
    
    private CellInfo[] cellInfos;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
    }
    
    public void map(CellInfo cell, Shape shape,
        OutputCollector<CellInfo, Shape> output, Reporter reporter)
        throws IOException {
      if (cell.cellId == -1) {
        // Output shape to all overlapping cells
        for (CellInfo cellInfo : cellInfos)
          if (cellInfo.isIntersected(shape))
            output.collect(cellInfo, shape);
      } else {
        // Input is already partitioned
        // Output shape to containing cell only
        output.collect(cell, shape);
      }
    }
  }
  
  /**
   * The reducer class draws an image for contents (shapes) in each cell info
   * @author Ahmed Eldawy
   *
   */
  public static class PlotReduce extends MapReduceBase
      implements Reducer<CellInfo, Shape, CellInfo, ImageWritable> {
    
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private boolean showBorders;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;
    private boolean showBlockCount;
    private boolean showRecordCount;
    private Path[] inputPaths;
    private FileSystem inFs;
    private boolean compactCells;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      inputPaths = FileInputFormat.getInputPaths(job);
      try {
        inFs = inputPaths[0].getFileSystem(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
      fileMbr = ImageOutputFormat.getFileMBR(job);
      imageWidth = ImageOutputFormat.getImageWidth(job);
      imageHeight = ImageOutputFormat.getImageHeight(job);
      showBorders = job.getBoolean(ShowBorders, false);
      showBlockCount = job.getBoolean(ShowBlockCount, false);
      showRecordCount = job.getBoolean(ShowRecordCount, false);
      compactCells = job.getBoolean(CompactCells, false);
      
      this.scale2 = (double)imageWidth * imageHeight /
          ((double)(fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));
    }

    @Override
    public void reduce(CellInfo cellInfo, Iterator<Shape> values,
        OutputCollector<CellInfo, ImageWritable> output, Reporter reporter)
        throws IOException {
      try {
        // Initialize the image
        int image_x1 = (int) ((cellInfo.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y1 = (int) ((cellInfo.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int image_x2 = (int) (((cellInfo.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y2 = (int) (((cellInfo.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int tile_width = image_x2 - image_x1;
        int tile_height = image_y2 - image_y1;
        if (tile_width == 0 || tile_height == 0)
          return;
        BufferedImage image = new BufferedImage(tile_width, tile_height,
            BufferedImage.TYPE_INT_ARGB);
        Color bg_color = new Color(0,0,0,0);
        Color stroke_color = Color.BLUE;

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(bg_color);
        graphics.clearRect(0, 0, tile_width, tile_height);
        graphics.setColor(stroke_color);
        graphics.translate(-image_x1, -image_y1);

        int recordCount = 0;
        
        double data_x1 = cellInfo.x1, data_y1 = cellInfo.y1,
            data_x2 = cellInfo.x2, data_y2 = cellInfo.y2;
        if (values.hasNext()) {
          recordCount++;
          Shape s = values.next();
          drawShape(graphics, s, fileMbr, imageWidth, imageHeight, scale2);
          if (compactCells) {
            Rectangle mbr = s.getMBR();
            data_x1 = mbr.x1;
            data_y1 = mbr.y1;
            data_x2 = mbr.x2;
            data_y2 = mbr.y2;
          }
          
          while (values.hasNext()) {
            recordCount++;
            s = values.next();
            drawShape(graphics, s, fileMbr, imageWidth, imageHeight, scale2);

            if (compactCells) {
              Rectangle mbr = s.getMBR();
              if (mbr.x1 < data_x1)
                data_x1 = mbr.x1;
              if (mbr.y1 < data_y1)
                data_y1 = mbr.y1;
              if (mbr.x2 > data_x2)
                data_x2 = mbr.x2;
              if (mbr.y2 > data_y2)
                data_y2 = mbr.y2;
            }
          }
        }
        
        if (compactCells) {
          // Ensure that the drawn rectangle fits in the partition
          if (data_x1 < cellInfo.x1)
            data_x1 = cellInfo.x1;
          if (data_y1 < cellInfo.y1)
            data_y1 = cellInfo.y1;
          if (data_x2 > cellInfo.x2)
            data_x2 = cellInfo.x2;
          if (data_y2 > cellInfo.y2)
            data_y2 = cellInfo.y2;
        }

        if (showBorders) {
          graphics.setColor(Color.BLACK);
          int rect_x1 = (int) ((data_x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
          int rect_y1 = (int) ((data_y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
          int rect_x2 = (int) ((data_x2 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
          int rect_y2 = (int) ((data_y2 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
          graphics.drawRect(rect_x1, rect_y1, rect_x2-rect_x1-1, rect_y2-rect_y1-1);
          String info = "";
          if (showRecordCount) {
            info += "rc:"+recordCount;
          }
          if (showBlockCount) {
            int blockCount = 0;
            for (Path inputPath : inputPaths) {
              FileStatus fileStatus = inFs.getFileStatus(inputPath);
              BlockLocation[] blocks = inFs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
              for (BlockLocation block : blocks) {
                if (block.getCellInfo().equals(cellInfo)) {
                  blockCount++;
                }
              }
            }
            if (info.length() != 0)
              info += ", ";
            info += "bc:"+blockCount;
          }
          if (info.length() > 0) {
            graphics.drawString(info, 0, graphics.getFontMetrics().getHeight());
          }
        }
        graphics.dispose();
        
        sharedValue.setImage(image);
        output.collect(cellInfo, sharedValue);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
  
  public static void drawShape(Graphics2D graphics, Shape s, Rectangle fileMbr,
      int imageWidth, int imageHeight, double scale2) {
    if (s instanceof Point) {
      Point pt = (Point) s;
      int x = (int) ((pt.x - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int y = (int) ((pt.y - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      
      if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
        graphics.fillRect(x, y, 1, 1);
    } else if (s instanceof Rectangle) {
      Rectangle r = (Rectangle) s;
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    } else if (s instanceof JTSShape) {
      JTSShape jts_shape = (JTSShape) s;
      Geometry geom = jts_shape.getGeom();
      Color shape_color = graphics.getColor();
      for (int i_geom = 0; i_geom < geom.getNumGeometries(); i_geom++) {
        Geometry sub_geom = geom.getGeometryN(i_geom);
        double sub_geom_alpha = sub_geom.getEnvelope().getArea() * scale2;
        int color_alpha = sub_geom_alpha > 1.0 ? 255 : (int) Math.round(sub_geom_alpha * 255);

        if (color_alpha == 0)
          continue;

        Coordinate[][] coordss;
        if (sub_geom instanceof Polygon) {
          Polygon poly = (Polygon) sub_geom;

          coordss = new Coordinate[1+poly.getNumInteriorRing()][];
          coordss[0] = poly.getExteriorRing().getCoordinates();
          for (int i = 0; i < poly.getNumInteriorRing(); i++) {
            coordss[i+1] = poly.getInteriorRingN(i).getCoordinates();
          }
        } else {
          coordss = new Coordinate[1][];
          coordss[0] = sub_geom.getCoordinates();
        }

        for (Coordinate[] coords : coordss) {
          int[] xpoints = new int[coords.length];
          int[] ypoints = new int[coords.length];
          int npoints = 0;

          // Transform all points in the polygon to image coordinates
          xpoints[npoints] = (int) Math.round((coords[0].x - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
          ypoints[npoints] = (int) Math.round((coords[0].y - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
          npoints++;
          for (int i_coord = 1; i_coord < coords.length; i_coord++) {
            int x = (int) Math.round((coords[i_coord].x - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
            int y = (int) Math.round((coords[i_coord].y - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
            if (x != xpoints[npoints-1] || y != ypoints[npoints-1]) {
              xpoints[npoints] = x;
              ypoints[npoints] = y;
              npoints++;
            }
          }
          // Draw the polygon
          if (color_alpha > 0) {
            graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
            graphics.drawPolygon(xpoints, ypoints, npoints);
          }
        }
      }
    } else {
      LOG.warn("Cannot draw a shape of type: "+s.getClass());
      Rectangle r = s.getMBR();
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      if (s_x1 >= 0 && s_x1 < imageWidth && s_y1 >= 0 && s_y1 < imageHeight)
        graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    }
  }
  
  public static <S extends Shape> void plotMapReduce(Path inFile, Path outFile,
      Shape shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)  throws IOException {
    JobConf job = new JobConf(Plot.class);
    job.setJobName("Plot");
    
    job.setMapperClass(PlotMap.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setReducerClass(PlotReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    job.setMapOutputKeyClass(CellInfo.class);
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    job.setMapOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(job);
    Rectangle fileMbr = FileMBR.fileMBRMapReduce(inFs, inFile, shape);
    FileStatus inFileStatus = inFs.getFileStatus(inFile);
    
    CellInfo[] cellInfos;
    if (inFs.getGlobalIndex(inFileStatus) == null) {
      // A heap file. The map function should partition the file
      GridInfo gridInfo = new GridInfo(fileMbr.x1, fileMbr.y1, fileMbr.x2,
          fileMbr.y2);
      gridInfo.calculateCellDimensions(inFileStatus.getLen(),
          inFileStatus.getBlockSize());
      cellInfos = gridInfo.getAllCells();
      // Doesn't make sense to show any partition information in a heap file
      showBorders = showBlockCount = showRecordCount = false;
    } else {
      // A grid file, use its cells
      Set<CellInfo> all_cells = new HashSet<CellInfo>();
      for (BlockLocation block :
          inFs.getFileBlockLocations(inFileStatus, 0, inFileStatus.getLen())) {
        if (block.getCellInfo() != null)
          all_cells.add(block.getCellInfo());
      }
      cellInfos = all_cells.toArray(new CellInfo[all_cells.size()]);
    }
    
    // Set cell information in the job configuration to be used by the mapper
    job.set(GridOutputFormat.OUTPUT_CELLS,
        GridOutputFormat.encodeCells(cellInfos));
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    ImageOutputFormat.setFileMBR(job, fileMbr);
    ImageOutputFormat.setImageWidth(job, width);
    ImageOutputFormat.setImageHeight(job, height);
    job.setBoolean(ShowBorders, showBorders);
    job.setBoolean(ShowBlockCount, showBlockCount);
    job.setBoolean(ShowRecordCount, showRecordCount);
    job.setBoolean(CompactCells, compactCells);
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    
    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    
    JobClient.runJob(job);
    
    // Rename output file
    // Combine all output files into one file as we do with grid files
    FileSystem outFs = outFile.getFileSystem(job);
    Path temp = new Path(outFile.toUri().getPath()+"_temp");
    outFs.rename(outFile, temp);
    FileStatus[] resultFiles = outFs.listStatus(temp, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toUri().getPath().contains("part-");
      }
    });
    
    if (resultFiles.length == 1) {
      // Only one output file
      outFs.rename(resultFiles[0].getPath(), outFile);
    } else {
      // Merge all images into one image (overlay)
      BufferedImage finalImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
      for (FileStatus resultFile : resultFiles) {
        FSDataInputStream imageFile = outFs.open(resultFile.getPath());
        BufferedImage tileImage = ImageIO.read(imageFile);
        imageFile.close();

        Graphics2D graphics;
        try {
          graphics = finalImage.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(finalImage);
        }
        graphics.drawImage(tileImage, 0, 0, null);
        graphics.dispose();
      }
      
      // Finally, write the resulting image to the given output path
      OutputStream outputImage = outFs.create(outFile);
      ImageIO.write(finalImage, "png", outputImage);
    }
    
    outFs.delete(temp, true);
  }
  
  public static <S extends Shape> void plotLocal(Path inFile, Path outFile,
      S shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    Rectangle fileMbr = FileMBR.fileMBRLocal(inFs, inFile, shape);
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    
    double scale2 = (double) width * height
        / ((double) (fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));

    // Create an image
    BufferedImage image = new BufferedImage(width, height,
        BufferedImage.TYPE_INT_ARGB);
    Graphics2D graphics = image.createGraphics();
    Color bg_color = new Color(0,0,0,0);
    bg_color = Color.WHITE;
    graphics.setBackground(bg_color);
    graphics.clearRect(0, 0, width, height);
    Color stroke_color = Color.BLACK;
    graphics.setColor(stroke_color);

    long fileLength = inFs.getFileStatus(inFile).getLen();
    ShapeRecordReader<S> reader =
      new ShapeRecordReader<S>(inFs.open(inFile), 0, fileLength);
    
    Rectangle cell = reader.createKey();
    while (reader.next(cell, shape)) {
      drawShape(graphics, shape, fileMbr, width, height, scale2);
    }
    
    reader.close();
    graphics.dispose();
    FileSystem outFs = outFile.getFileSystem(new Configuration());
    OutputStream out = outFs.create(outFile, true);
    ImageIO.write(image, "png", out);
    out.close();
  }
  
  public static <S extends Shape> void plot(Path inFile, Path outFile,
      S shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFs.getFileBlockLocations(inFStatus, 0, inFStatus.getLen()).length > 3) {
      plotMapReduce(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    } else {
      plotLocal(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    }
  }

  
  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|jts> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000,1000)");
    System.out.println("height:<h> - Maximum height of the image (1000,1000)");
//    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("-borders: For globally indexed files, draws the borders of partitions");
    System.out.println("-showblockcount: For globally indexed files, draws the borders of partitions");
    System.out.println("-showrecordcount: Show number of records in each partition");
    System.out.println("-compact: Shrink partition boundaries to fit contents");
    System.out.println("-overwrite: Override output file without notice");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Plot.class);
    Path[] files = cla.getPaths();
    if (files.length < 2) {
      printUsage();
      throw new RuntimeException("Illegal arguments. File names missing");
    }
    
    Path inFile = files[0];
    FileSystem inFs = inFile.getFileSystem(conf);
    if (!inFs.exists(inFile)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    boolean overwrite = cla.isOverwrite();
    Path outFile = files[1];
    FileSystem outFs = outFile.getFileSystem(conf);
    if (outFs.exists(outFile)) {
      if (overwrite)
        outFs.delete(outFile, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    boolean showBorders = cla.is("borders");
    boolean showBlockCount = cla.is("showblockcount");
    boolean showRecordCount = cla.is("showrecordcount");
    boolean compactCells = cla.is("compact");
    Shape shape = cla.getShape(true);
    
    int width = cla.getWidth(1000);
    int height = cla.getHeight(1000);

    if (cla.isLocal()) {
      plotLocal(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    } else {
      plotMapReduce(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    }
  }

}
