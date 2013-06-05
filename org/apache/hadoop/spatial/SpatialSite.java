package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.spatial.ShapeRecordReader;

/**
 * Combines all the configuration needed for SpatialHadoop.
 * @author eldawy
 *
 */
public class SpatialSite {

  /**Enforce static only calls*/
  private SpatialSite() {}
  
  /**The class used to filter blocks before starting map tasks*/
  public static final String FilterClass = "spatialHadoop.mapreduce.filter";
  
  /**The default RTree degree used for local indexing*/
  public static final String RTREE_DEGREE = "spatialHadoop.storage.RTreeDegree";
  
  /**Maximum size of an RTree.*/
  public static final String LOCAL_INDEX_BLOCK_SIZE =
      "spatialHadoop.storage.LocalIndexBlockSize";
  
  /**Whether to build the RTree in fast mode or slow (memory saving) mode.*/
  public static final String RTREE_BUILD_MODE =
      "spatialHadoop.storage.RTreeBuildMode";
  
  /**Configuration line to set the default shape class to use if not set*/
  public static final String SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.ShapeRecordReader.ShapeClass.default";
  
  /**Configuration line name for replication overhead*/
  public static final String INDEXING_OVERHEAD =
      "spatialHadoop.storage.IndexingOverhead";
  
  /**Ratio of the sample to read from files to build a global R-tree*/
  public static final String SAMPLE_RATIO = "spatialHadoop.storage.SampleRatio";
  
  /**Ratio of the sample to read from files to build a global R-tree*/
  public static final String SAMPLE_SIZE = "spatialHadoop.storage.SampleSize";
  
  /**Inform index writers to pack cells around its contents upon write*/
  public static final String PACK_CELLS = "spatialHadoop.storage.pack";
  
  /**
   * A marker put in the beginning of each block to indicate that this block
   * is stored as an RTree. It might be better to store this in the BlockInfo
   * in a field (e.g. localIndexType).
   */
  public static final long RTreeFileMarker = -0x00012345678910L;

  /**
   * Maximum number of shapes to read in one read operation and return when
   * reading a file as array
   */
  public static final String MaxShapesInOneRead =
      "spatialHadoop.mapred.MaxShapesPerRead";

  /**
   * Maximum size in bytes that can be read in one read operation
   */
  public static final String MaxBytesInOneRead =
      "spatialHadoop.mapred.MaxBytesPerRead";

  public static byte[] RTreeFileMarkerB;
  
  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeLong(RTreeFileMarker);
      dout.close();
      bout.close();
      RTreeFileMarkerB = bout.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Creates a stock shape according to the given configuration
   * @param job
   * @return
   */
  public static Shape createStockShape(Configuration job) {
    Shape stockShape = null;
    String shapeClassName = job.get(SHAPE_CLASS, Point.class.getName());
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      stockShape = shapeClass.newInstance();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return stockShape;
  }
  
  public static void setShape(Configuration conf, String param, Shape shape) {
    String str = shape.getClass().getName() + ",";
    str += shape.toText(new Text()).toString();
    conf.set(param, str);
  }
  
  public static Shape getShape(Configuration conf, String param) {
    String str = conf.get(param);
    String[] parts = str.split(",", 2);
    String shapeClassName = parts[0];
    Shape shape = null;
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      shape = shapeClass.newInstance();
      shape.fromText(new Text(parts[1]));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return shape;
  }

  public static GlobalIndex<Partition> getGlobalIndex(FileSystem fs,
      Path dir) throws IOException {
    Path masterFile = new Path(dir, "_master");
    // Check if the given file is indexed
    if (!fs.exists(masterFile))
      return null;
    ShapeRecordReader<Partition> reader = new ShapeRecordReader<Partition>(
        fs.open(masterFile), 0, fs.getFileStatus(masterFile).getLen());
    CellInfo dummy = new CellInfo();
    Partition partition = new Partition();
    ArrayList<Partition> partitions = new ArrayList<Partition>();
    while (reader.next(dummy, partition)) {
      partitions.add(partition.clone());
    }
    GlobalIndex<Partition> globalIndex = new GlobalIndex<Partition>();
    globalIndex.bulkLoad(partitions.toArray(new Partition[partitions.size()]));
    return globalIndex;
  }

  public static boolean isRTree(FileSystem fs, Path path) throws IOException {
    FileStatus file = fs.getFileStatus(path);
    Path fileToCheck;
    if (file.isDir()) {
      // Check any cell (first cell)
      InputStream in = fs.open(new Path(path, "_master"));
      ShapeRecordReader<Partition> reader = new ShapeRecordReader<Partition>(in, 0, Long.MAX_VALUE);
      Partition first_partition = new Partition();
      if (!reader.next(new CellInfo(), first_partition)) {
        in.close();
        throw new RuntimeException("Cannot find any partitions in "+path);
      }
      in.close();
      fileToCheck = new Path(path, first_partition.filename);
    } else {
      fileToCheck = file.getPath();
    }
    FSDataInputStream fsdis = fs.open(fileToCheck);
    Long signature = fsdis.readLong();
    fsdis.close();
    return signature == SpatialSite.RTreeFileMarker;
  }
  
  public static CellInfo[] cellsOf(FileSystem fs, Path path) throws IOException {
    GlobalIndex<Partition> gindex = getGlobalIndex(fs, path);
    if (gindex == null)
      return null;
    
    // Find all partitions of the given file. If two partitions overlap,
    // we consider the union of them. This case corresponds to an R-tree
    // index where a partition is stored as multiple R-tree. Since we compact
    // each R-tree when it is stored, different compactions might lead to
    // different partitions all overlapping the same area. In this case, we
    // union them to ensure the whole area is covered without having overlaps
    // between returned partitions.
    
    ArrayList<CellInfo> cellSet = new ArrayList<CellInfo>();
    for (Partition p : gindex) {
      boolean overlapping = false;
      for (int i = 0; i < cellSet.size(); i++) {
        if (p.isIntersected(cellSet.get(i))) {
          if (overlapping == true)
            throw new RuntimeException("Overlapping partitions");
          overlapping = true;
          cellSet.get(i).expand(p);
        }
      }
      if (overlapping == false) {
        cellSet.add(new CellInfo(cellSet.size() + 1, p));
      }
    }
    return cellSet.toArray(new CellInfo[cellSet.size()]);

  }
}
