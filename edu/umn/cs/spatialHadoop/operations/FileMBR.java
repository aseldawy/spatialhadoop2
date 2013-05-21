package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.spatial.ShapeInputFormat;
import org.apache.hadoop.mapred.spatial.ShapeRecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SimpleSpatialIndex;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Finds the minimal bounding rectangle for a file.
 * @author Ahmed Eldawy
 *
 */
public class FileMBR {
  private static final NullWritable Dummy = NullWritable.get();
  private static final Rectangle MBR = new Rectangle();

  public static class Map extends MapReduceBase implements
      Mapper<CellInfo, Shape, NullWritable, Rectangle> {
    public void map(CellInfo dummy, Shape shape,
        OutputCollector<NullWritable, Rectangle> output, Reporter reporter)
        throws IOException {
      Rectangle mbr = shape.getMBR();
      MBR.set(mbr.x, mbr.y, mbr.width, mbr.height);
      output.collect(Dummy, MBR);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<NullWritable, Rectangle, NullWritable, Rectangle> {
    @Override
    public void reduce(NullWritable dummy, Iterator<Rectangle> values,
        OutputCollector<NullWritable, Rectangle> output, Reporter reporter)
            throws IOException {
      if (values.hasNext()) {
        Rectangle rect = values.next();
        
        double y1 = rect.getY1();
        double x2 = rect.getX2();
        double y2 = rect.getY2();
        double x1 = rect.getX1();

        while (values.hasNext()) {
          rect = values.next();
          if (rect.getX1() < x1) x1 = rect.getX1();
          if (rect.getY1() < y1) y1 = rect.getY1();
          if (rect.getX2() > x2) x2 = rect.getX2();
          if (rect.getY2() > y2) y2 = rect.getY2();
        }
        output.collect(dummy, new Rectangle(x1, y1, x2 - x1, y2 - y1));
      }
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param conf
   * @param fs
   * @param file
   * @return
   * @throws IOException 
   */
  public static <S extends Shape> Rectangle fileMBRMapReduce(FileSystem fs,
      Path file, S stockShape) throws IOException {
    // Quickly get file MBR if it is globally indexed
    SimpleSpatialIndex<CellInfo> globalIndex = SpatialSite.getGlobalIndex(fs, file);
    if (globalIndex != null) {
      return globalIndex.getMBR();
    }
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath =
        new Path("/"+file.getName()+".mbr_"+(int)(Math.random()*1000000));
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("FileMBR");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Rectangle.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setInputFormat(ShapeInputFormat.class);
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
    
    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    Rectangle mbr = new Rectangle();
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
        Text text = new Text();
        if (lineReader.readLine(text) > 0) {
          mbr.fromText(text);
        }
        lineReader.close();
      }
    }
    
    outFs.delete(outputPath, true);
    
    return mbr;
  }
  
  /**
   * Counts the exact number of lines in a file by opening the file and
   * reading it line by line
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static <S extends Shape> Rectangle fileMBRLocal(FileSystem fs,
      Path file, S stockShape) throws IOException {
    // Try to get file MBR from the MBRs of blocks
    BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(
        fs.getFileStatus(file), 0, fs.getFileStatus(file).getLen());
    if (fileBlockLocations[0].getCellInfo() != null) {
      boolean heap_file = false;
      BlockLocation firstBlock = fileBlockLocations[0];
      double x1 = firstBlock.getCellInfo().getX1();
      double y1 = firstBlock.getCellInfo().getY1();
      double x2 = firstBlock.getCellInfo().getX2();
      double y2 = firstBlock.getCellInfo().getY2();

      for (BlockLocation blockLocation : fileBlockLocations) {
        Rectangle rect = blockLocation.getCellInfo();
        if (blockLocation.getCellInfo() == null) {
          heap_file = true;
          break;
        }
        if (rect.getX1() < x1) x1 = rect.getX1();
        if (rect.getY1() < y1) y1 = rect.getY1();
        if (rect.getX2() > x2) x2 = rect.getX2();
        if (rect.getY2() > y2) y2 = rect.getY2();
      }
      if (!heap_file) {
        return new Rectangle(x1, y1, x2-x1, y2-y1);
      }
    }
    long file_size = fs.getFileStatus(file).getLen();
    
    ShapeRecordReader<Shape> shapeReader =
        new ShapeRecordReader<Shape>(fs.open(file), 0, file_size);

    
    CellInfo key = shapeReader.createKey();
    
    if (!shapeReader.next(key, stockShape)) {
      shapeReader.close();
      return null;
    }
      
    Rectangle rect = stockShape.getMBR();
    double x1 = rect.getX1();
    double y1 = rect.getY1();
    double x2 = rect.getX2();
    double y2 = rect.getY2();

    while (shapeReader.next(key, stockShape)) {
      rect = stockShape.getMBR();
      if (rect.getX1() < x1) x1 = rect.getX1();
      if (rect.getY1() < y1) y1 = rect.getY1();
      if (rect.getX2() > x2) x2 = rect.getX2();
      if (rect.getY2() > y2) y2 = rect.getY2();
    }
    return new Rectangle(x1, y1, x2-x1, y2-y1);
  }
  
  private static void printUsage() {
    System.out.println("Finds the MBR of an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    Path inputFile = cla.getPath();
    if (inputFile == null) {
      printUsage();
      return;
    }
    
    FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      printUsage();
      return;
    }

    Shape stockShape = cla.getShape(true);
    boolean local = cla.isLocal();
    Rectangle mbr = local ? fileMBRLocal(fs, inputFile, stockShape) :
      fileMBRMapReduce(fs, inputFile, stockShape);
    System.out.println("MBR of records in file "+inputFile+" is "+mbr);
  }

}
