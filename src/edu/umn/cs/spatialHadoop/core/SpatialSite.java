package edu.umn.cs.spatialHadoop.core;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Combines all the configuration needed for SpatialHadoop.
 * @author Ahmed Eldawy
 *
 */
public class SpatialSite {
  
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(SpatialSite.class);

  /**Enforce static only calls*/
  private SpatialSite() {}
  
  /**The class used to filter blocks before starting map tasks*/
  public static final String FilterClass = "spatialHadoop.mapreduce.filter";
  
  /**Maximum size of an RTree.*/
  public static final String LOCAL_INDEX_BLOCK_SIZE =
      "spatialHadoop.storage.LocalIndexBlockSize";
  
  /**Whether to build the RTree in fast mode or slow (memory saving) mode.*/
  public static final String RTREE_BUILD_MODE =
      "spatialHadoop.storage.RTreeBuildMode";
  
  /**Configuration line to set the default shape class to use if not set*/
  public static final String ShapeClass = "SpatialSite.ShapeClass";
  
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
  
  public static final String OUTPUT_CELLS = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.CellsInfo";
  public static final String OVERWRITE = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.Overwrite";
  public static final String RTREE = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.RTree";

  
  private static final CompressionCodecFactory compressionCodecs =
      new CompressionCodecFactory(new Configuration());

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

  /**Expand global index partitions to cover all of its contents*/
  public static final String EXPAND_CELLS = "spatialHadoop.storage.expand";

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
  
  public static void setShapeClass(Configuration conf, Class<? extends Shape> klass) {
    conf.setClass(ShapeClass, klass, Shape.class);
  }
  
  public static Class<? extends Shape> getShapeClass(Configuration conf) {
    return conf.getClass(ShapeClass, Point.class, Shape.class);
  }
  
  /**
   * Creates a stock shape according to the given configuration
   * @param job
   * @return
   */
  public static Shape createStockShape(Configuration job) {
    Shape stockShape = null;
    try {
      Class<? extends Shape> shapeClass = getShapeClass(job);
      stockShape = shapeClass.newInstance();
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
    // Retrieve the master file (the only file with the name _master in it)
    FileStatus[] masterFiles = fs.listStatus(dir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().contains("_master");
      }
    });
    // Check if the given file is indexed
    if (masterFiles.length == 0)
      return null;
    if (masterFiles.length > 1)
      throw new RuntimeException("Found more than one master file in "+dir);
    Path masterFile = masterFiles[0].getPath();
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
    globalIndex.setCompact(masterFile.getName().endsWith("rtree") || masterFile.getName().endsWith("r+tree"));
    globalIndex.setReplicated(masterFile.getName().endsWith("r+tree") || masterFile.getName().endsWith("grid"));
    return globalIndex;
  }

  public static boolean isRTree(FileSystem fs, Path path) throws IOException {
    FileStatus file = fs.getFileStatus(path);
    Path fileToCheck;
    if (file.isDir()) {
      // Check any cell (e.g., first cell)
      GlobalIndex<Partition> gIndex = getGlobalIndex(fs, path);
      fileToCheck = new Path(path, gIndex.iterator().next().filename);
    } else {
      fileToCheck = file.getPath();
    }
    InputStream fileIn = fs.open(fileToCheck);
    
    // Check if file is compressed
    CompressionCodec codec = compressionCodecs.getCodec(fileToCheck);
    Decompressor decompressor = null;
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
      fileIn = codec.createInputStream(fileIn, decompressor);
    }
    byte[] signature = new byte[RTreeFileMarkerB.length];
    fileIn.read(signature);
    fileIn.close();
    if (decompressor != null) {
      CodecPool.returnDecompressor(decompressor);
    }
    return Arrays.equals(signature, SpatialSite.RTreeFileMarkerB);
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
  
  public static void setCells(JobConf job, CellInfo[] cellsInfo) throws IOException {
    Path tempFile;
    FileSystem fs = FileSystem.get(job);
    do {
      tempFile = new Path(job.getJobName()+"_"+(int)(Math.random()*1000000)+".cells");
    } while (fs.exists(tempFile));
    FSDataOutputStream out = fs.create(tempFile);
    out.writeInt(cellsInfo.length);
    for (CellInfo cell : cellsInfo) {
      cell.write(out);
    }
    out.close();
    
    fs.deleteOnExit(tempFile);

    DistributedCache.addCacheFile(tempFile.toUri(), job);
    job.set(OUTPUT_CELLS, tempFile.getName());
  }
  
  public static CellInfo[] getCells(JobConf job) throws IOException {
    CellInfo[] cells = null;
    String cells_file = job.get(OUTPUT_CELLS);
    if (cells_file != null) {
      Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
      for (Path cacheFile : cacheFiles) {
        if (cacheFile.getName().contains(cells_file)) {
          FSDataInputStream in = FileSystem.getLocal(job).open(cacheFile);
          
          int cellCount = in.readInt();
          cells = new CellInfo[cellCount];
          for (int i = 0; i < cellCount; i++) {
            cells[i] = new CellInfo();
            cells[i].readFields(in);
          }
          
          in.close();
        }
      }
    }
    return cells;
  }

  
  public static void setRectangle(Configuration conf, String name, Rectangle rect) {
    conf.set(name, rect.toText(new Text()).toString());
  }
  
  /**
   * Retrieves a rectangle from configuration parameter
   * @param conf
   * @param name
   * @return
   */
  public static Rectangle getRectangle(Configuration conf, String name) {
    Rectangle rect = null;
    String rectStr = conf.get(name);
    if (rectStr != null) {
      rect = new Rectangle();
      rect.fromText(new Text(rectStr));
    }
    return rect;
  }

}
