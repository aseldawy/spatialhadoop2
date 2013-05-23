package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.spatial.ShapeLineInputFormat;
import org.apache.hadoop.mapred.spatial.ShapeRecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.JTSShape;
import org.apache.hadoop.spatial.SpatialSite;
import org.postgis.jts.JtsBinaryWriter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Computes the union of a set of shapes given a category for each shape.
 * Input:
 *  - A file that contains all shapes (one per line)
 *  - A file that contains a category for each shape
 * Output:
 *  - One file that contains one line per category. Each line contains category
 *    ID as it appears in the second file and the union of all shapes assigned
 *    to this ID
 * 
 * @author eldawy
 *
 */
public class Union {
  private static final Log LOG = LogFactory.getLog(Union.class);
  
  static class GeometryArray {
    String categoryId;
    Vector<Geometry> geometries = new Vector<Geometry>();
    
    @Override
    public String toString() {
      return categoryId+": "+geometries;
    }
  }
  
  static class UnionMapper extends MapReduceBase
      implements Mapper<CellInfo, Text, IntWritable, Text> {
    /**
     * Maps each shape by ID to a category
     */
    private Map<Integer, Integer> idToCategory = new HashMap<Integer, Integer>();
    
    /**
     * A shared key for all intermediate data
     */
    IntWritable sharedKey = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      try {
        Path categoryFile = DistributedCache.getLocalCacheFiles(job)[0];
        readCategories(categoryFile, idToCategory);
      } catch (IOException e) {
        LOG.error(e);
        e.printStackTrace();
      }
    }
    
    @Override
    public void map(CellInfo key, Text line,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      byte[] bytes = line.getBytes();
      int column_to_read = 6;
      int i1_shape = 0, i2_shape = 0;
      int i1 = 0;
      int i2 = 0;
      for (int i = 0; i <= column_to_read; i++) {
        byte delimiter = ',';
        if (bytes[i1] == '\'' || bytes[i1] == '\"')
          delimiter = bytes[i1++];
        i2 = i1+1;
        while (bytes[i2] != delimiter)
          i2++;
        if (i == 0) {
          i1_shape = i1;
          i2_shape = i2;
          if (delimiter != ',') {
            // If the first column is quoted, include the quotes
            i1--;
            i2++;
          }
        }
        if (i < column_to_read) {
          // Skip to next column
          i1 = i2 + 1; // Skip the terminator
          if (delimiter != ',') // Skip another byte if field is quoted 
            i1++;
        }
      }
      int shape_zip = TextSerializerHelper.deserializeInt(bytes, i1, i2-i1);
      Integer category = idToCategory.get(shape_zip);
      if (category != null) {
        sharedKey.set(category);
        // Truncate the line to include only the shape
        line.set(bytes, i1_shape, i2_shape-i1_shape);
        output.collect(sharedKey, line);
      }
    }
  }
  
  /**
   * Reduce function takes a category and union all shapes in that category
   * @author eldawy
   *
   */
  static class UnionReducer extends MapReduceBase
      implements Reducer<IntWritable, Text, IntWritable, Text> {
    
    private Text temp_out = new Text();
    
    @Override
    public void reduce(IntWritable category, Iterator<Text> shape_lines,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      JTSShape shape = new JTSShape();
      Vector<Geometry> shapes = new Vector<Geometry>();
      while (shape_lines.hasNext()) {
        shape.fromText(shape_lines.next());
        shapes.add(shape.getGeom());
      }
      Geometry[] geometries = shapes.toArray(new Geometry[shapes.size()]);
      GeometryCollection geo_collection = new GeometryCollection(geometries,
          geometries[0].getFactory());
      Geometry union = geo_collection.buffer(0);
      geo_collection = null;
      geometries = null;
      shapes = null;
      temp_out.clear();
      output.collect(category, new JTSShape(union).toText(temp_out));
    }
  }
  
  /**
   * Calculates the union of a set of shapes categorized by some user defined
   * category.
   * @param shapeFile - Input file that contains shapes
   * @param categoryFile - Category file that contains the category of each
   *  shape.Shapes not appearing in this file are not generated in output.
   * @param output - An output file that contains each category and the union
   *  of all shapes in it. Each line contains the category, then a comma,
   *   then the union represented as text.
   * @throws IOException
   */
  public static void unionMapReduce(Path shapeFile, Path categoryFile,
      Path output, boolean overwrite) throws IOException {
    JobConf job = new JobConf(Union.class);
    job.setJobName("Union");

    // Check output file existence
    FileSystem outFs = output.getFileSystem(job);
    if (outFs.exists(output)) {
      if (overwrite) {
        outFs.delete(output, true);
      } else {
        throw new RuntimeException("Output path already exists and -overwrite flag is not set");
      }
    }
    
    // Set map and reduce
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));
    
    job.setMapperClass(UnionMapper.class);
    job.setCombinerClass(UnionReducer.class);
    job.setReducerClass(UnionReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Set input and output
    job.setInputFormat(ShapeLineInputFormat.class);
    TextInputFormat.addInputPath(job, shapeFile);
    DistributedCache.addCacheFile(categoryFile.toUri(), job);
    
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Start job
    JobClient.runJob(job);
  }

  /**
   * Calculates the union of a set of shapes categorized by some user defined
   * category.
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static Map<Integer, Geometry> unionLocal(
      Path shapeFile, Path categoryFile) throws IOException {
    long t1 = System.currentTimeMillis();
    // 1- Build a hashtable of categories (given their size is small)
    Map<Integer, Integer> idToCategory = new HashMap<Integer, Integer>();
    readCategories(categoryFile, idToCategory);
    long t2 = System.currentTimeMillis();
    
    // 2- Read shapes from the shape file and relate each one to a category
    
    // Prepare a hash that stores shapes in each category
    Map<Integer, Vector<Geometry>> categoryShapes =
      new HashMap<Integer, Vector<Geometry>>();
    
    FileSystem fs1 = shapeFile.getFileSystem(new Configuration());
    long file_size1 = fs1.getFileStatus(shapeFile).getLen();
    
    ShapeRecordReader<JTSShape> shapeReader =
        new ShapeRecordReader<JTSShape>(fs1.open(shapeFile), 0, file_size1);
    CellInfo cellInfo = new CellInfo();
    JTSShape shape = new JTSShape();

    while (shapeReader.next(cellInfo, shape)) {
      int shape_zip = Integer.parseInt(shape.getExtra().split(",", 7)[5]);
      Integer category = idToCategory.get(shape_zip);
      if (category != null) {
        Vector<Geometry> geometries = categoryShapes.get(category);
        if (geometries == null) {
          geometries = new Vector<Geometry>();
          categoryShapes.put(category, geometries);
        }
        geometries.add(shape.getGeom());
      }
    }
    
    shapeReader.close();
    long t3 = System.currentTimeMillis();

    // 3- Find the union of each category
    JtsBinaryWriter jts_binary_writer = new JtsBinaryWriter();
    Map<Integer, Geometry> final_result = new HashMap<Integer, Geometry>();
    for (Map.Entry<Integer, Vector<Geometry>> category :
          categoryShapes.entrySet()) {
      if (!category.getValue().isEmpty()) {
        Geometry[] geometries = category.getValue().toArray(
            new Geometry[category.getValue().size()]);
        for (Geometry geom : geometries)
          System.out.println(jts_binary_writer.writeHexed(geom));
        GeometryCollection geom_collection = new GeometryCollection(geometries,
            geometries[0].getFactory());
        Geometry union = geom_collection.buffer(0);
        final_result.put(category.getKey(), union);
        // Free up some memory
        category.getValue().clear();
      }
    }
    long t4 = System.currentTimeMillis();

    System.out.println("Time reading categories: "+(t2-t1)+" millis");
    System.out.println("Time reading records: "+(t3-t2)+" millis");
    System.out.println("Time union categories: "+(t4-t3)+" millis");

    return final_result;
  }

  /**
   * Read all categories from the category file
   * @param categoryFile
   * @param categoryShapes
   * @param idToCategory
   * @throws IOException
   */
  private static void readCategories(Path categoryFile,
      Map<Integer, Integer> idToCategory) throws IOException {
    Map<Integer, String> idToCatName = new HashMap<Integer, String>();
    FileSystem fsCategory = FileSystem.getLocal(new Configuration());
    long categoryFileSize = fsCategory.getFileStatus(categoryFile).getLen();
    if (categoryFileSize > 1024*1024)
      LOG.warn("Category file size is big: "+categoryFileSize);
    InputStream inCategory = fsCategory.open(categoryFile);
    LineRecordReader lineReader = new LineRecordReader(inCategory, 0,
        categoryFileSize, new Configuration());
    LongWritable lineOffset = lineReader.createKey();
    Text line = lineReader.createValue();
    
    Set<String> catNames = new TreeSet<String>();
    
    while (lineReader.next(lineOffset, line)) {
      int shape_id = TextSerializerHelper.consumeInt(line, ',');
      String cat_name = line.toString();
      catNames.add(cat_name);
      idToCatName.put(shape_id, cat_name);
    }

    lineReader.close();
    
    // Change category names to numbers
    Map<String, Integer> cat_name_to_id = new HashMap<String, Integer>();
    int cat_id = 0;
    for (String cat_name : catNames) {
      cat_name_to_id.put(cat_name, cat_id++);
    }
    
    for (Map.Entry<Integer, String> entry : idToCatName.entrySet()) {
      idToCategory.put(entry.getKey(), cat_name_to_id.get(entry.getValue()));
    }
  }
  
  private static void printUsage() {
    System.out.println("Finds the union of all shapes in a file according to some category.");
    System.out.println("The output is a list of categories with the union of all shapes in each category.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<shape file>: (*) Path to file that contains all shapes");
    System.out.println("<category file>: (*) Path to a file that contains the category of each shape");
    System.out.println("<output file>: (*) Path to output file.");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Union.class);
    Path[] allFiles = cla.getPaths();
    boolean local = cla.isLocal();
    boolean overwrite = cla.isOverwrite();
    if (allFiles.length < 2) {
      printUsage();
      throw new RuntimeException("Illegal arguments. Input file missing");
    }
    
    for (int i = 0; i < allFiles.length - 1; i++) {
      Path inputFile = allFiles[i];
      FileSystem fs = inputFile.getFileSystem(conf);
      if (!fs.exists(inputFile)) {
        printUsage();
        throw new RuntimeException("Input file does not exist");
      }
    }

    long t1 = System.currentTimeMillis();
    if (local) {
      Map<Integer, Geometry> union = unionLocal(allFiles[0], allFiles[1]);
//    for (Map.Entry<Text, Geometry> category : union.entrySet()) {
//      System.out.println(category.getValue().toText()+","+category);
//    }
    } else {
      unionMapReduce(allFiles[0], allFiles[1], allFiles[2], overwrite);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }

}
