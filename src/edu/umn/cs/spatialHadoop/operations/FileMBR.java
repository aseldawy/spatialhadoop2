package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Finds the minimal bounding rectangle for a file.
 * @author Ahmed Eldawy
 *
 */
public class FileMBR {
  private static final NullWritable Dummy = NullWritable.get();

  public static class Map extends MapReduceBase implements
      Mapper<Rectangle, Shape, NullWritable, Rectangle> {
    
    private final Rectangle MBR = new Rectangle();
    private Rectangle mbr_so_far = null;
    
    public void map(Rectangle dummy, Shape shape,
        OutputCollector<NullWritable, Rectangle> output, Reporter reporter)
        throws IOException {
      Rectangle mbr = shape.getMBR();
      
      if (mbr_so_far == null) {
        MBR.set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
        mbr_so_far = new Rectangle();
        mbr_so_far.set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
        output.collect(Dummy, MBR);
      } else {
        // Skip writing file to output
        if (!mbr_so_far.contains(mbr)) {
          mbr_so_far.expand(mbr);
          output.collect(Dummy, MBR);
        }
      }
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
        
        double x1 = rect.x1;
        double y1 = rect.y1;
        double x2 = rect.x2;
        double y2 = rect.y2;

        while (values.hasNext()) {
          rect = values.next();
          if (rect.x1 < x1) x1 = rect.x1;
          if (rect.y1 < y1) y1 = rect.y1;
          if (rect.x2 > x2) x2 = rect.x2;
          if (rect.y2 > y2) y2 = rect.y2;
        }
        output.collect(dummy, new Rectangle(x1, y1, x2, y2));
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
    GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(fs, file);
    if (globalIndex != null) {
      return globalIndex.getMBR();
    }
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(file.toUri().getPath()+".mbr_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
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
    // Try to get file MBR from the global index (if possible)
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, file);
    if (gindex != null) {
      return gindex.getMBR();
    }
    long file_size = fs.getFileStatus(file).getLen();
    
    ShapeRecordReader<Shape> shapeReader = new ShapeRecordReader<Shape>(
        new Configuration(), new FileSplit(file, 0, file_size, new String[] {}));

    Rectangle key = shapeReader.createKey();
    
    if (!shapeReader.next(key, stockShape)) {
      shapeReader.close();
      return null;
    }
      
    Rectangle rect = stockShape.getMBR();
    double x1 = rect.x1;
    double y1 = rect.y1;
    double x2 = rect.x2;
    double y2 = rect.y2;

    while (shapeReader.next(key, stockShape)) {
      rect = stockShape.getMBR();
      if (rect.x1 < x1) x1 = rect.x1;
      if (rect.y1 < y1) y1 = rect.y1;
      if (rect.x2 > x2) x2 = rect.x2;
      if (rect.y2 > y2) y2 = rect.y2;
    }
    return new Rectangle(x1, y1, x2-x1, y2-y1);
  }
  
  public static Rectangle fileMBR(FileSystem fs, Path inFile, Shape stockShape) throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 1) {
      // Either a directory of file or a large file
      return fileMBRMapReduce(fs, inFile, stockShape);
    } else {
      // A single small file, process it without MapReduce
      return fileMBRLocal(fs, inFile, stockShape);
    }
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
    long t1 = System.currentTimeMillis();
    Rectangle mbr = fileMBRMapReduce(fs, inputFile, stockShape);
    long t2 = System.currentTimeMillis();
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MBR of records in file "+inputFile+" is "+mbr);
  }

}
