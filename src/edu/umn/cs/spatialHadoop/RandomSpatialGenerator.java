package edu.umn.cs.spatialHadoop;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author Ahmed Eldawy
 *
 */
public class RandomSpatialGenerator {
  static byte[] NEW_LINE;
  
  static {
    try {
      NEW_LINE = System.getProperty("line.separator").getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
  
  public enum DistributionType {
    UNIFORM, GAUSSIAN, CORRELATED, ANTI_CORRELATED
  }

  public final static double rho = 0.9;
  
  /**
   * Generates a grid file in the output file system. This function uses
   * either GridRecordWriter or RTreeGridRecordWriter according to the last
   * parameter. The size of the generated file depends mainly on the number
   * of cells in the generated file and the distribution of the data. The size
   * itself doesn't make much sense because there might be some cells that are
   * not filled up with data while still having the same file size. Think of it
   * as a cabinet with some empty drawers. The cabinet size is still the same,
   * but what really makes sense here is the actual files stored in the cabinet.
   * For this, we use the total size as a hint and it means actually the
   * accumulated size of all generated shapes. It is the size of the file if
   * it were generated as a heap file. This also makes the size of the output
   * file (grid file) comparable with that of the heap file.
   * @param outFS
   * @param outFilePath
   * @param mbr
   * @param shape 
   * @param totalSize
   * @param blocksize - Size of each block in the generated file
   * @param rtree
   * @throws IOException
   */
  public static void generateGridFile(FileSystem outFS, Path outFilePath,
      Shape shape, final long totalSize, final Rectangle mbr,
      DistributionType type, int rectSize, long seed, long blocksize,
      String gindex, String lindex, boolean overwrite) throws IOException {
    if (outFS.exists(outFilePath)) {
      if (overwrite) {
        outFS.delete(outFilePath, true);
      } else {
        throw new RuntimeException("Output file already exists and -overwrite flag is not set");
      }
    }
    outFS.mkdirs(outFilePath);
    
    GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    Configuration conf = outFS.getConf();
    final double IndexingOverhead =
        conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f);
    // Serialize one shape and see how many characters it takes
    final Random random = new Random(seed);
    final Text text = new Text();
    if (blocksize == 0)
      blocksize = outFS.getDefaultBlockSize(outFilePath);
    int num_of_cells = (int) Math.ceil(totalSize * (1+IndexingOverhead) /
        blocksize);
    CellInfo[] cellInfo;
    
    if (gindex == null) {
      throw new RuntimeException("Unsupported global index: "+gindex);
    } else if (gindex.equals("grid")) {
      gridInfo.calculateCellDimensions(num_of_cells);
      cellInfo = gridInfo.getAllCells();
    } else {
      throw new RuntimeException("Unsupported global index: "+gindex);
    }
    
    ShapeRecordWriter<Shape> recordWriter;
    if (lindex == null) {
      recordWriter = new GridRecordWriter<Shape>(outFilePath, null, null, cellInfo, gridInfo.equals("rtree"));
      ((GridRecordWriter<Shape>)recordWriter).setBlockSize(blocksize);
    } else if (lindex.equals("rtree")) {
      recordWriter = new RTreeGridRecordWriter<Shape>(outFilePath, null, null, cellInfo, gridInfo.equals("rtree"));
      recordWriter.setStockObject(shape);
      ((RTreeGridRecordWriter<Shape>)recordWriter).setBlockSize(blocksize);
    } else {
      throw new RuntimeException("Unsupported local index: " + lindex);
    }

    long generatedSize = 0;
    if (rectSize == 0)
      rectSize = 100;
    
    long t1 = System.currentTimeMillis();
    while (true) {
      // Generate a random rectangle
      generateShape(shape, mbr, type, rectSize, random);

      // Serialize it to text first to make it easy count its size
      text.clear();
      shape.toText(text);
      if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
        break;
      
      recordWriter.write(NullWritable.get(), shape);
      
      generatedSize += text.getLength() + NEW_LINE.length;
    }
    long t2 = System.currentTimeMillis();
    recordWriter.close(null);
    long t3 = System.currentTimeMillis();
    System.out.println("Core time: "+(t2-t1)+" millis");
    System.out.println("Close time: "+(t3-t2)+" millis");
  }
  
  /**
   * Generates random rectangles and write the result to a file.
   * @param outFS - The file system that contains the output file
   * @param outputFile - The file name to write to. If either outFS or
   *   outputFile is null, data is generated to the standard output
   * @param mbr - The whole MBR to generate in
   * @param shape 
   * @param totalSize - The total size of the generated file
   * @param blocksize 
   * @throws IOException 
   */
  public static void generateHeapFile(FileSystem outFS, Path outputFilePath,
      Shape shape, long totalSize, Rectangle mbr, DistributionType type,
      int rectSize, long seed, long blocksize, boolean overwrite)
      throws IOException {
    OutputStream out = null;
    if (blocksize == 0 && outFS != null)
      blocksize = outFS.getDefaultBlockSize(outputFilePath);
    if (outFS == null || outputFilePath == null)
      out = new BufferedOutputStream(System.out);
    else
      out = new BufferedOutputStream(outFS.create(outputFilePath, true,
          outFS.getConf().getInt("io.file.buffer.size", 4096),
          outFS.getDefaultReplication(outputFilePath), blocksize));
    long generatedSize = 0;
    Random random = new Random(seed);
    Text text = new Text();
    
    if (rectSize == 0)
      rectSize = 100;
    long t1 = System.currentTimeMillis();
    while (true) {
      // Generate a random rectangle
      generateShape(shape, mbr, type, rectSize, random);
      
      // Serialize it to text first to make it easy count its size
      text.clear();
      shape.toText(text);
      if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
        break;
      byte[] bytes = text.getBytes();
      out.write(bytes, 0, text.getLength());
      out.write(NEW_LINE);
      generatedSize += text.getLength() + NEW_LINE.length;
    }
    long t2 = System.currentTimeMillis();
    
    // Cannot close standard output
    if (outFS != null && outputFilePath != null)
      out.close();
    else
      out.flush();
    long t3 = System.currentTimeMillis();
    if (outFS == null || outputFilePath == null) {
      System.err.println("Core time: "+(t2-t1)+" millis");
      System.err.println("Close time: "+(t3-t2)+" millis");
    } else {
      System.out.println("Core time: "+(t2-t1)+" millis");
      System.out.println("Close time: "+(t3-t2)+" millis");
    }
  }

  private static void generateShape(Shape shape, Rectangle mbr,
      DistributionType type, int rectSize, Random random) {
    if (shape instanceof Point) {
      generatePoint((Point)shape, mbr, type, random);
    } else if (shape instanceof Rectangle) {
      ((Rectangle)shape).x1 = random.nextDouble() * (mbr.x2 - mbr.x1) + mbr.x1;
      ((Rectangle)shape).y1 = random.nextDouble() * (mbr.y2 - mbr.y1) + mbr.y1;
      ((Rectangle)shape).x2 = Math.min(mbr.x2, ((Rectangle)shape).x1 + random.nextInt(rectSize) + 2);
      ((Rectangle)shape).y2 = Math.min(mbr.y2, ((Rectangle)shape).y1 + random.nextInt(rectSize) + 2);
    } else {
      throw new RuntimeException("Cannot generate random shapes of type: "+shape.getClass());
    }
  }
  // The standard deviation is 0.2
  public static double nextGaussian(Random rand) {
    double res = 0;
    do {
      res = rand.nextGaussian() / 5.0;
    } while(res < -1 || res > 1);
    return res;
  }
  
  public static void generatePoint(Point p, Rectangle mbr, DistributionType type, Random rand) {
    double x, y;
    switch (type) {
    case UNIFORM:
      p.x = rand.nextDouble() * (mbr.x2 - mbr.x1) + mbr.x1;
      p.y = rand.nextDouble() * (mbr.y2 - mbr.y1) + mbr.y1; 
      break;
    case GAUSSIAN:
      p.x = nextGaussian(rand) * (mbr.x2 - mbr.x1) / 2.0 + (mbr.x1 + mbr.x2) / 2.0;
      p.y = nextGaussian(rand) * (mbr.y2 - mbr.y1) / 2.0 + (mbr.y1 + mbr.y2) / 2.0;
      break;
    case CORRELATED:
    case ANTI_CORRELATED:
      x = rand.nextDouble() * 2 - 1;
      do {
        y = rho * x + Math.sqrt(1 - rho * rho) * nextGaussian(rand);
      } while(y < -1 || y > 1) ;
      p.x = x * (mbr.x2 - mbr.x1) / 2.0 + (mbr.x1 + mbr.x2) / 2.0;
      p.y = y * (mbr.y2 - mbr.y1) / 2.0 + (mbr.y1 + mbr.y2) / 2.0;
      if (type == DistributionType.ANTI_CORRELATED)
        p.y = mbr.y2 - (p.y - mbr.y1);
      break;
    default:
      throw new RuntimeException("Unrecognized distribution type: "+type);
    }
  }
  
  
  private static void printUsage() {
    System.out.println("Generates a file with random shapes");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<output file> - Path to the file to generate. If omitted, file is generated to stdout.");
    System.out.println("mbr:<x,y,w,h> - (*) The MBR of the generated data. Originated at (x,y) with dimensions (w,h)");
    System.out.println("shape:<point|(rectangle)|polygon> - Type of shapes in generated file");
    System.out.println("blocksize:<size> - Block size in the generated file");
    System.out.println("global:<grid|rtree> - Type of global index in generated file");
    System.out.println("local:<grid|rtree> - Type of local index in generated file");
    System.out.println("seed:<s> - Use a specific seed to generate the file");
    System.out.println("-overwrite - Overwrite output file without notice");
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(RandomSpatialGenerator.class);
    CommandLineArguments cla = new CommandLineArguments(args);
    Rectangle mbr = cla.getRectangle();
    if (mbr == null) {
      printUsage();
      throw new RuntimeException("Set MBR of the generated file using rect:<x,y,w,h>");
    }

    Path outputFile = cla.getPath();
    FileSystem fs = outputFile != null? outputFile.getFileSystem(conf) : null;
    Shape stockShape = cla.getShape(false);
    long blocksize = cla.getBlockSize();
    int rectSize = cla.getRectSize();
    long seed = cla.getSeed();
    if (stockShape == null)
      stockShape = new Rectangle();
    
    long totalSize = cla.getSize();
    String gindex = cla.getGIndex();
    String lindex = cla.getLIndex();
    boolean overwrite = cla.isOverwrite();
    
    DistributionType type = DistributionType.UNIFORM;
    String strType = cla.get("type");
    if (strType != null) {
      strType = strType.toLowerCase();
      if (strType.startsWith("uni"))
        type = DistributionType.UNIFORM;
      else if (strType.startsWith("gaus"))
        type = DistributionType.GAUSSIAN;
      else if (strType.startsWith("cor"))
        type = DistributionType.CORRELATED;
      else if (strType.startsWith("anti"))
        type = DistributionType.ANTI_CORRELATED;
      else {
        System.err.println("Unknown distribution type: "+cla.get("type"));
        printUsage();
        fs.close();
        return;
      }
    }

    if (outputFile != null) {
      System.out.print("Generating a file ");
      System.out.print("with gindex:"+gindex+" ");
      System.out.print("with lindex:"+lindex+" ");
      System.out.println("file of size: "+totalSize);
      System.out.println("To: " + outputFile);
      System.out.println("In the range: " + mbr);
    }
    if (gindex == null && lindex == null)
      generateHeapFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, overwrite);
    else
      generateGridFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, gindex, lindex, overwrite);
  }

}
