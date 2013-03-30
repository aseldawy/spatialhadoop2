package edu.umn.cs.spatialHadoop.operations;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;

public class Plot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(Plot.class);
  
  /**Whether or not to show partition borders (boundaries) in generated image*/
  private static final String ShowBorders = "plot.show_borders";

  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author eldawy
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
        // Output shape to containing cell only
        output.collect(cell, shape);
      }
    }
  }
  
  /**
   * The combiner class draws an image for contents (shapes) in each cell info
   * @author eldawy
   *
   */
  public static class PlotReduce extends MapReduceBase
      implements Reducer<CellInfo, Shape, CellInfo, ImageWritable> {
    
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private boolean show_borders;
    private ImageWritable sharedValue = new ImageWritable();

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      fileMbr = ImageOutputFormat.getFileMBR(job);
      imageWidth = ImageOutputFormat.getImageWidth(job);
      imageHeight = ImageOutputFormat.getImageHeight(job);
      show_borders = job.getBoolean(ShowBorders, false);
    }

    @Override
    public void reduce(CellInfo cellInfo, Iterator<Shape> values,
        OutputCollector<CellInfo, ImageWritable> output, Reporter reporter)
        throws IOException {
      // Initialize the image
      int image_x1 = (int) ((cellInfo.x - fileMbr.x) * imageWidth / fileMbr.width);
      int image_y1 = (int) ((cellInfo.y - fileMbr.y) * imageHeight / fileMbr.height);
      int image_x2 = (int) (((cellInfo.x + cellInfo.width) - fileMbr.x) * imageWidth / fileMbr.width);
      int image_y2 = (int) (((cellInfo.y + cellInfo.height) - fileMbr.y) * imageHeight / fileMbr.height);
      int tile_width = image_x2 - image_x1;
      int tile_height = image_y2 - image_y1;
      BufferedImage image = new BufferedImage(tile_width, tile_height,
          BufferedImage.TYPE_INT_ARGB);
      Graphics2D graphics = image.createGraphics();
      Color bg_color = new Color(0,0,0,0);
      graphics.setBackground(bg_color);
      graphics.clearRect(0, 0, tile_width, tile_height);
      graphics.setColor(Color.black);
      
      if (show_borders)
        graphics.drawRect(0, 0, tile_width, tile_height);
      
      // Plot all shapes on that image
      while (values.hasNext()) {
        Shape s = values.next();
        // TODO draw the shape according to its type
        
        Rectangle s_mbr = s.getMBR();
        int s_x1 = (int) ((s_mbr.x - fileMbr.x) * imageWidth / fileMbr.width);
        int s_y1 = (int) ((s_mbr.y - fileMbr.y) * imageHeight / fileMbr.height);
        int s_x2 = (int) (((s_mbr.x + s_mbr.width) - fileMbr.x) * imageWidth / fileMbr.width);
        int s_y2 = (int) (((s_mbr.y + s_mbr.height) - fileMbr.y) * imageHeight / fileMbr.height);
        graphics.drawRect(s_x1 - image_x1, s_y1 - image_y1,
            s_x2 - s_x1 + 1, s_y2-s_y1 + 1);
      }
      
      graphics.dispose();
      
      sharedValue.setImage(image);
      output.collect(cellInfo, sharedValue);
    }
  }
  
  public static <S extends Shape> void plotMapReduce(Path inFile, Path outFile,
      Shape shape, int width, int height, boolean borders)  throws IOException {
    JobConf job = new JobConf(Plot.class);
    job.setJobName("Plot");
    
    job.setMapperClass(PlotMap.class);
    job.setReducerClass(PlotReduce.class);
    job.setMapOutputKeyClass(CellInfo.class);
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    job.setMapOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(job);
    Rectangle fileMbr = FileMBR.fileMBRLocal(inFs, inFile, shape);
    FileStatus inFileStatus = inFs.getFileStatus(inFile);
    
    CellInfo[] cellInfos;
    if (inFs.getGlobalIndex(inFileStatus) == null) {
      // A heap file. The map function should partition the file
      GridInfo gridInfo = new GridInfo(fileMbr.x, fileMbr.y, fileMbr.width,
          fileMbr.height);
      gridInfo.calculateCellDimensions(inFileStatus.getLen(),
          inFileStatus.getBlockSize());
      cellInfos = gridInfo.getAllCells();
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
    if ((double)fileMbr.width / fileMbr.height > (double) width / height) {
      // Fix width and change height
      height = (int) (fileMbr.height * width / fileMbr.width);
    } else {
      width = (int) (fileMbr.width * height / fileMbr.height);
    }
    ImageOutputFormat.setFileMBR(job, fileMbr);
    ImageOutputFormat.setImageWidth(job, width);
    ImageOutputFormat.setImageHeight(job, height);
    job.setBoolean(ShowBorders, borders);
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    
    // TODO change TextOutputFormat to ImageOutputFormat (to be created)
    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    
    JobClient.runJob(job);
  }
  
  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|jts> - (*) Type of shapes stored in input file");
    System.out.println("image:<w,h> - Maximum width and height of the image (1000,1000)");
    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("-borders: For globally indexed files, draws the borders of partitions");
    System.out.println("-overwrite: Override output file without notice");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Plot.class);
    Path[] files = cla.getPaths();
    if (files.length < 2) {
      printUsage();
      throw new RuntimeException("Illegal arguments. File names missing");
    }
    
    Path inputFile = files[0];
    FileSystem inFs = inputFile.getFileSystem(conf);
    if (!inFs.exists(inputFile)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    boolean overwrite = cla.isOverwrite();
    Path outputFile = files[1];
    FileSystem outFs = outputFile.getFileSystem(conf);
    if (outFs.exists(outputFile)) {
      if (overwrite)
        outFs.delete(outputFile, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    boolean borders = cla.isBorders();
    Shape shape = cla.getShape(true);
    
    int width = cla.getWidth(1000);
    int height = cla.getHeight(1000);
    
    plotMapReduce(inputFile, outputFile, shape, width, height, borders);
  }

}
