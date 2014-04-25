/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.OGCESRIShape;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Computes the union of all shapes in a given input file.
 * 
 * @author Ahmed Eldawy
 *
 */
public class Union {
  private static final Log LOG = LogFactory.getLog(Union.class);
  
  /**
   * Reduce function takes a category and union all shapes in that category
   * @author eldawy
   *
   */
  static class UnionReducer<S extends OGCJTSShape> extends MapReduceBase
      implements Reducer<IntWritable, S, IntWritable, OGCJTSShape> {
    
    @Override
    public void reduce(IntWritable dummy, Iterator<S> shapes,
        OutputCollector<IntWritable, OGCJTSShape> output, Reporter reporter)
        throws IOException {
      final int threshold = 500000;
      Geometry[] shapes_list = new Geometry[threshold];
      int size = 0;
      while (shapes.hasNext()) {
        OGCJTSShape shape = shapes.next();
        shapes_list[size++] = shape.geom;
        if (size == threshold) {
          LOG.info("Computing union of "+size+" shapes");
          reporter.progress();
          GeometryCollection geo_collection = new GeometryCollection(shapes_list, shapes_list[0].getFactory());
          Geometry union = geo_collection.buffer(0);
          geo_collection = null;
          size = 0;
          shapes_list[size++] = union;
        }
      }

      LOG.info("Final union computation of "+size+" shapes");
      Geometry[] good_shapes = new Geometry[size];
      System.arraycopy(shapes_list, 0, good_shapes, 0, size);
      GeometryCollection geo_collection = new GeometryCollection(good_shapes, good_shapes[0].getFactory());
      Geometry union = geo_collection.buffer(0);
      geo_collection = null;
      shapes = null;
      reporter.progress();
      LOG.info("Writing geoms to output");
      if (union instanceof GeometryCollection) {
        GeometryCollection union_shapes = (GeometryCollection) union;
        for (int i_geom = 0; i_geom < union_shapes.getNumGeometries(); i_geom++) {
          Geometry geom_n = union_shapes.getGeometryN(i_geom);
          output.collect(dummy, new OGCJTSShape(geom_n));
        }
      } else {
        output.collect(dummy, new OGCJTSShape(union));
      }
      LOG.info("Done writing geoms to output");
    }
  }
  
  /**
   * Calculates the union of a set of shapes.
   * @param inFile - Input file that contains shapes
   * @param output - An output file that contains each category and the union
   *  of all shapes in it. Each line contains the category, then a comma,
   *   then the union represented as text.
   * @throws IOException
   */
  public static void unionMapReduce(Path inFile, Path output,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(Union.class);
    job.setJobName("Union");
    boolean overwrite = params.is("overwrite");
    Shape shape = params.getShape("shape");

    // Check output file existence
    FileSystem outFs = output.getFileSystem(job);
    if (outFs.exists(output)) {
      if (overwrite) {
        outFs.delete(output, true);
      } else {
        throw new RuntimeException("Output path already exists and -overwrite flag is not set");
      }
    }
    
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(inFile.getFileSystem(job), inFile);
    int reduceBuckets = 1;
    if (gindex != null) {
      int groups = Math.max(1, (int) Math.sqrt(gindex.size() / 4));
      // Ensure that the final number of buckets is less than current
      while (groups * groups >= gindex.size() && groups > 1) {
        groups--;
      }
      
      // Get the list of all partitions ordered by x
      CellInfo[] unionGroups = new CellInfo[groups * groups];
      Partition[] all = new Partition[gindex.size()];
      int i = 0;
      for (Partition p : gindex)
        all[i++] = p;
      Arrays.sort(all, new Comparator<Partition>() {
        @Override
        public int compare(Partition o1, Partition o2) {
          return o1.x1 < o2.x1 ? -1 : 1;
        }
      });
      
      int i1 = 0;
      i = 0;
      while (i1 < all.length) {
        int i2 = Math.min(all.length, (i + 1) * all.length / groups);
        Arrays.sort(all, i1, i2, new Comparator<Partition>() {
          @Override
          public int compare(Partition o1, Partition o2) {
            return o1.y1 < o2.y1 ? -1 : 1;
          }
        });
        
        int j = 0;
        int j1 = i1;
        while (j1 < i2) {
          int j2 = Math.min(i2, (j + 1) * i2 / groups);
          
          CellInfo r = new CellInfo(j * groups + i + 1, all[j1]);
          while (j1 < j2) {
            j1++;
            if (j1 < j2)
              r.expand(all[j1]);
          }
          unionGroups[j * groups + i] = r;
          
          j++;
        }
        
        i++;
        i1 = i2;
      }
      
      SpatialSite.setCells(job, unionGroups);
      reduceBuckets = unionGroups.length;
    } else {
      Rectangle mbr = FileMBR.fileMBR(inFile, params);
      CellInfo[] unionGroups = new CellInfo[1];
      unionGroups[0] = new CellInfo(1, mbr);
      SpatialSite.setCells(job, unionGroups);
      reduceBuckets = unionGroups.length;
    }
    LOG.info("Number of reduce buckets: "+reduceBuckets);

    // Set map and reduce
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, Math.min(reduceBuckets, clusterStatus.getMaxReduceTasks() * 9 / 10)));

    job.setMapperClass(Repartition.RepartitionMap.class);
    job.setCombinerClass(UnionReducer.class);
    job.setReducerClass(UnionReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());

    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    TextInputFormat.addInputPath(job, inFile);
    
    job.setOutputFormat(GridOutputFormat.class);
    GridOutputFormat.setOutputPath(job, output);

    // Start job
    JobClient.runJob(job);
    
    // TODO If outputPath not set by user, automatically delete it
//    if (userOutputPath == null)
//      outFs.delete(outputPath, true);
  }
  
  
  /**
   * Convert a string containing a hex string to a byte array of binary.
   * For example, the string "AABB" is converted to the byte array {0xAA, 0XBB}
   * @param hex
   * @return
   */
  public static byte[] hexToBytes(String hex) {
    byte[] bytes = new byte[(hex.length() + 1) / 2];
    for (int i = 0; i < hex.length(); i++) {
      byte x = (byte) hex.charAt(i);
      if (x >= '0' && x <= '9')
        x -= '0';
      else if (x >= 'a' && x <= 'f')
        x = (byte) ((x - 'a') + 0xa);
      else if (x >= 'A' && x <= 'F')
        x = (byte) ((x - 'A') + 0xA);
      else
        throw new RuntimeException("Invalid hex char "+x);
      if (i % 2 == 0)
        x <<= 4;
      bytes[i / 2] |= x;
    }
    return bytes;
  }

  
  public static <S extends OGCJTSShape> Geometry unionStream(S shape) throws IOException {
    ShapeRecordReader<S> reader =
        new ShapeRecordReader<S>(System.in, 0, Long.MAX_VALUE);
    final int threshold = 5000000;
    ArrayList<Geometry> polygons = new ArrayList<Geometry>();
    
    Rectangle key = new Rectangle();

    while (reader.next(key, shape)) {
      polygons.add(shape.geom);
      if (polygons.size() >= threshold) {
        GeometryCollection collection = new GeometryCollection(polygons.toArray(new Geometry[polygons.size()]), polygons.get(0).getFactory());
        Geometry union = collection.buffer(0);
        polygons.clear();
        polygons.add(union);
        System.err.println("Size: "+polygons.size());
      }
    }
    GeometryCollection collection = new GeometryCollection(polygons.toArray(new Geometry[polygons.size()]), polygons.get(0).getFactory());
    Geometry union = collection.buffer(0);
    polygons.clear();
    return union;
  }

  /**
   * Calculates the union of a set of shapes
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static Geometry unionLocal(Path inFile, OperationsParams params)
      throws IOException {
    Shape shape = params.getShape("shape");
    // Read shapes from the shape file and relate each one to a category
    
    // Prepare a hash that stores all shapes
    Vector shapes;
    if (shape instanceof OGCJTSShape) {
      shapes = new Vector<Geometry>();
    } else if (shape instanceof OGCESRIShape) {
      shapes = new Vector<OGCGeometry>();
    } else {
      throw new RuntimeException("Cannot union shapes of type '"+shape.getClass()+"'");
    }
    
    FileSystem fs = inFile.getFileSystem(new Configuration());
    long file_size = fs.getFileStatus(inFile).getLen();
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();
    JobConf job = new JobConf(params);
    ShapeInputFormat.addInputPath(job, inFile);
    InputSplit[] splits = inputFormat.getSplits(job, 1);

    double progress = 0;
    for (InputSplit split : splits) {
      ShapeRecordReader<Shape> shapeReader =
          new ShapeRecordReader<Shape>(job, (FileSplit)split);
      Rectangle partition = new Rectangle();
      
      // Number of entries before we perform a partial union
      int threshold = 1000;
      
      while (shapeReader.next(partition, shape)) {
        if (shape instanceof OGCJTSShape)
          shapes.add(((OGCJTSShape)shape).geom);
        else if (shape instanceof OGCESRIShape)
          shapes.add(((OGCESRIShape)shape).geom);
        
        if (shapes.size() > threshold ) {
          System.out.print("Calculating union for "+shapes.size()+" shapes ... ");
          // Find the union of all shapes
          if (shape instanceof OGCJTSShape) {
            GeometryCollection all_geoms = new GeometryCollection((Geometry[])shapes.toArray(new Geometry[shapes.size()]), ((Geometry)shapes.firstElement()).getFactory());
            
            Geometry union = all_geoms.buffer(0);
            shapes.clear();
            shapes.add(union);
          } else {
            OGCConcreteGeometryCollection all_geoms = new OGCConcreteGeometryCollection(shapes, ((OGCGeometry)shapes.firstElement()).getEsriSpatialReference());
            OGCGeometry union = all_geoms.union((OGCGeometry)shapes.firstElement());
            shapes.clear();
            shapes.add(union);
          }
          System.out.println(((int)((progress + shapeReader.getProgress() / splits.length)*100))+"% done");
        }
      }
      shapeReader.close();
      progress += 1.0f / splits.length;
    }

    // Find the union of all shapes
    if (shape instanceof OGCJTSShape) {
      GeometryCollection all_geoms = new GeometryCollection((Geometry[])shapes.toArray(new Geometry[shapes.size()]), ((Geometry)shapes.firstElement()).getFactory());
      
      Geometry union = all_geoms.buffer(0);
      return union;
    } else {
      OGCConcreteGeometryCollection all_geoms = new OGCConcreteGeometryCollection(shapes, ((OGCGeometry)shapes.firstElement()).getEsriSpatialReference());
      OGCGeometry union = all_geoms.union((OGCGeometry)shapes.firstElement());
      try {
        return new WKTReader().read(union.asText());
      } catch (ParseException e) {
        throw new RuntimeException("Error converting result", e);
      }
    }
  }

  private static void printUsage() {
    System.out.println("Finds the union of all shapes in the input file.");
    System.out.println("The output is one shape that represents the union of all shapes in input file.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<shape file>: (*) Path to file that contains all shapes");
    System.out.println("<output file>: (*) Path to output file.");
  }

  public static void union(Path inFile, Path outFile,
      OperationsParams params) throws IOException {
    FileSystem inFs = inFile.getFileSystem(params);
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    boolean autoLocal = !(inFStatus.isDir() ||
        inFStatus.getLen() / inFStatus.getBlockSize() > 3);
    Boolean isLocal = params.is("local", autoLocal);
    
    if (isLocal) {
      unionLocal(inFile, params);
    } else {
      unionMapReduce(inFile, outFile, params);
    }
  }


  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (params.getPaths().length == 0) {
      if (params.is("local")) {
        long t1 = System.currentTimeMillis();
        unionStream((OGCJTSShape)params.getShape("shape"));
        long t2 = System.currentTimeMillis();
        System.err.println("Total time for union: "+(t2-t1)+" millis");
        return;
      }
      printUsage();
      throw new RuntimeException("Illegal arguments. Input file missing");
    }
    
    if (!params.checkInputOutput()) {
      printUsage();
      return;
    }

    Path inputPath = params.getInputPath();
    Path outputPath = params.getOutputPath();
    
    long t1 = System.currentTimeMillis();
    union(inputPath, outputPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }

}
