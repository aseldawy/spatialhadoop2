package edu.umn.cs.spatialHadoop;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.spatial.GridRecordWriter;
import org.apache.hadoop.mapred.spatial.RTreeGridRecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Polygon;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.ShapeRecordWriter;
import org.apache.hadoop.spatial.SpatialSite;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author eldawy
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
      Shape shape, final long totalSize, final Rectangle mbr, int rectSize,
      long seed,
      long blocksize, String gindex, String lindex, boolean overwrite) throws IOException {
    GridInfo gridInfo = new GridInfo(mbr.x, mbr.y, mbr.width, mbr.height);
    Configuration conf = outFS.getConf();
    final double IndexingOverhead =
        conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f);
    // Serialize one shape and see how many characters it takes
    final Random random = new Random(seed);
    final Text text = new Text();
    if (blocksize == 0)
      blocksize = outFS.getDefaultBlockSize();
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
      recordWriter = new GridRecordWriter(outFS, outFilePath, cellInfo,
          overwrite);
      ((GridRecordWriter)recordWriter).setBlockSize(blocksize);
    } else if (lindex.equals("rtree")) {
      recordWriter = new RTreeGridRecordWriter(outFS, outFilePath, cellInfo,
          overwrite);
      recordWriter.setStockObject(shape);
      ((RTreeGridRecordWriter)recordWriter).setBlockSize(blocksize);
    } else {
      throw new RuntimeException("Unsupported local index: " + lindex);
    }

    long generatedSize = 0;
    if (rectSize == 0)
      rectSize = 100;
    
    int minPoints = 3, maxPoints = 5;
    
    long t1 = System.currentTimeMillis();
    while (true) {
      // Generate a random rectangle
      generateShape(shape, mbr, rectSize, random, minPoints, maxPoints);

      // Serialize it to text first to make it easy count its size
      text.clear();
      shape.toText(text);
      if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
        break;
      
      recordWriter.write(shape, text);
      
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
      Shape shape, long totalSize, Rectangle mbr, int rectSize, long seed,
      long blocksize, boolean overwrite) throws IOException {
    OutputStream out = null;
    if (blocksize == 0 && outFS != null)
      blocksize = outFS.getDefaultBlockSize();
    if (outFS == null || outputFilePath == null)
      out = new BufferedOutputStream(System.out);
    else
      out = new BufferedOutputStream(outFS.create(outputFilePath, true,
          outFS.getConf().getInt("io.file.buffer.size", 4096),
          outFS.getDefaultReplication(), blocksize));
    long generatedSize = 0;
    Random random = new Random(seed);
    Text text = new Text();
    
    // Range for number of points to generate in case of polygons (inclusive)
    int minPoints = 3, maxPoints = 5;
    
    if (rectSize == 0)
      rectSize = 100;
    long t1 = System.currentTimeMillis();
    while (true) {
      // Generate a random rectangle
      generateShape(shape, mbr, rectSize, random, minPoints, maxPoints);
      
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

  private static void generateShape(Shape shape, Rectangle mbr, int rectSize,
      Random random, int minPoints, int maxPoints) {
    if (shape instanceof Point) {
      ((Point)shape).x = Math.abs(random.nextLong()) % mbr.width + mbr.x;
      ((Point)shape).y = Math.abs(random.nextLong()) % mbr.height + mbr.y;
    } else if (shape instanceof Rectangle) {
      long x = ((Rectangle)shape).x = Math.abs(random.nextLong()) % mbr.width + mbr.x;
      long y = ((Rectangle)shape).y = Math.abs(random.nextLong()) % mbr.height + mbr.y;
      ((Rectangle)shape).width = Math.min(Math.abs(random.nextLong()) % rectSize,
          mbr.width + mbr.x - x);
      ((Rectangle)shape).height = Math.min(Math.abs(random.nextLong()) % rectSize,
          mbr.height + mbr.y - y);
    } else if (shape instanceof Polygon) {
      int npoints = random.nextInt(maxPoints - minPoints + 1) + minPoints;
      int xpoints[] = new int[npoints];
      int ypoints[] = new int[npoints];
      
      int x = xpoints[0] = (int) (Math.abs(random.nextInt((int)mbr.width)) + mbr.x);
      int y = ypoints[0] = (int) (Math.abs(random.nextInt((int)mbr.height)) + mbr.y);
      for (int i = 1; i < npoints; i++) {
        xpoints[i] = (int) Math.min(Math.abs(random.nextInt(rectSize)),
            mbr.width + mbr.x - x);
        ypoints[i] = (int) Math.min(Math.abs(random.nextInt(rectSize)),
            mbr.height + mbr.y - y);
      }
      ((Polygon)shape).set(xpoints, ypoints, npoints);
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

    
    if (outputFile != null) {
      System.out.print("Generating a file ");
      System.out.print("with gindex:"+gindex+" ");
      System.out.print("with lindex:"+lindex+" ");
      System.out.println("file of size: "+totalSize);
      System.out.println("To: " + outputFile);
      System.out.println("In the range: " + mbr);
    }
    if (gindex == null && lindex == null)
      generateHeapFile(fs, outputFile, stockShape, totalSize, mbr, rectSize, seed, blocksize, overwrite);
    else
      generateGridFile(fs, outputFile, stockShape, totalSize, mbr, rectSize, seed, blocksize, gindex, lindex, overwrite);
  }

}
