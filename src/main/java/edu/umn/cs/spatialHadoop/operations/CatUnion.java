/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.OGCESRIShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

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
 * @author Ahmed Eldawy
 *
 */
public class CatUnion {
  private static final Log LOG = LogFactory.getLog(CatUnion.class);
  
  static class GeometryArray {
    String categoryId;
    Vector<OGCGeometry> geometries = new Vector<OGCGeometry>();
    
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
      OGCESRIShape shape = new OGCESRIShape();
      Vector<OGCGeometry> shapes = new Vector<OGCGeometry>();
      while (shape_lines.hasNext()) {
        shape.fromText(shape_lines.next());
        shapes.add(shape.geom);
      }
      OGCGeometryCollection geo_collection = new OGCConcreteGeometryCollection(shapes,
          shapes.firstElement().getEsriSpatialReference());
      OGCGeometry union = geo_collection.union(shapes.firstElement());
      geo_collection = null;
      shapes = null;
      temp_out.clear();
      output.collect(category, new OGCESRIShape(union).toText(temp_out));
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
      Path output, OperationsParams params) throws IOException {
    
    JobConf job = new JobConf(params, CatUnion.class);
    job.setJobName("Union");

    // Check output file existence
    FileSystem outFs = output.getFileSystem(job);
    if (outFs.exists(output)) {
      if (params.getBoolean("overwrite", false)) {
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
   * @param shapeFile
   * @param categoryFile
   * @return
   * @throws IOException
   */
  public static Map<Integer, OGCGeometry> unionLocal(
      Path shapeFile, Path categoryFile) throws IOException {
    long t1 = System.currentTimeMillis();
    // 1- Build a hashtable of categories (given their size is small)
    Map<Integer, Integer> idToCategory = new HashMap<Integer, Integer>();
    readCategories(categoryFile, idToCategory);
    long t2 = System.currentTimeMillis();
    
    // 2- Read shapes from the shape file and relate each one to a category
    
    // Prepare a hash that stores shapes in each category
    Map<Integer, Vector<OGCGeometry>> categoryShapes =
      new HashMap<Integer, Vector<OGCGeometry>>();
    
    FileSystem fs1 = shapeFile.getFileSystem(new Configuration());
    long file_size1 = fs1.getFileStatus(shapeFile).getLen();
    
    ShapeIterRecordReader shapeReader =
        new ShapeIterRecordReader(fs1.open(shapeFile), 0, file_size1);
    Rectangle cellInfo = shapeReader.createKey();
    ShapeIterator shapes = shapeReader.createValue();

    while (shapeReader.next(cellInfo, shapes)) {
      for (Shape shape : shapes) {
        //int shape_zip = Integer.parseInt(shape.extra.split(",", 7)[5]);
        //Integer category = idToCategory.get(shape_zip);
        Integer category = null;
        if (category != null) {
          Vector<OGCGeometry> geometries = categoryShapes.get(category);
          if (geometries == null) {
            geometries = new Vector<OGCGeometry>();
            categoryShapes.put(category, geometries);
          }
          geometries.add(((OGCESRIShape)shape).geom);
        }
      }
    }
    
    shapeReader.close();
    long t3 = System.currentTimeMillis();

    // 3- Find the union of each category
    Map<Integer, OGCGeometry> final_result = new HashMap<Integer, OGCGeometry>();
    for (Map.Entry<Integer, Vector<OGCGeometry>> category :
          categoryShapes.entrySet()) {
      if (!category.getValue().isEmpty()) {
        OGCGeometryCollection geom_collection = new OGCConcreteGeometryCollection(category.getValue(),
            category.getValue().firstElement().esriSR);
        OGCGeometry union = geom_collection.union(category.getValue().firstElement());
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
    
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    OperationsParams params = new OperationsParams(parser);
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    
    Path[] allFiles = params.getPaths();
    if (allFiles.length < 3) {
      LOG.error("Error! This operations requires two input paths and one output path");
      printUsage();
      System.exit(1);
    }

    long t1 = System.currentTimeMillis();
    // TODO automatically decide whether to do local or not if not explicitly specified
    if (params.getBoolean("local", false)) {
      Map<Integer, OGCGeometry> union = unionLocal(allFiles[0], allFiles[1]);
//    for (Map.Entry<Text, Geometry> category : union.entrySet()) {
//      System.out.println(category.getValue().toText()+","+category);
//    }
    } else {
      unionMapReduce(allFiles[0], allFiles[1], allFiles[2], params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }

}
