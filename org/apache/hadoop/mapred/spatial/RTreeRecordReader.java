package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;


/**
 * Reads a file as a list of RTrees
 * @author Ahmed Eldawy
 *
 */
public class RTreeRecordReader<S extends Shape> extends SpatialRecordReader<Rectangle, RTree<S>> {
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private S stockShape;
  
  public RTreeRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    stockShape = createStockShape(conf);
  }
  
  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    stockShape = createStockShape(job);
  }

  public RTreeRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, RTree<S> rtree) throws IOException {
    boolean read_line = nextRTree(rtree);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public RTree<S> createValue() {
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockShape);
    return rtree;
  }
  
  @SuppressWarnings("unchecked")
  private S createStockShape(Configuration job) {
    S stockShape = null;
    String shapeClassName =
        job.get(SpatialSite.SHAPE_CLASS, Point.class.getName());
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      stockShape = (S) shapeClass.newInstance();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return stockShape;
  }
}
