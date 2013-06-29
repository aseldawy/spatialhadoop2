package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;


/**
 * Reads a file as a list of RTrees
 * @author Ahmed Eldawy
 *
 */
public class ShapeArrayRecordReader extends SpatialRecordReader<Rectangle, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ShapeArrayRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private Class<? extends Shape> shapeClass;
  
  public ShapeArrayRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    shapeClass = SpatialSite.getShapeClass(conf);
  }
  
  public ShapeArrayRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    shapeClass = SpatialSite.getShapeClass(job);
  }

  public ShapeArrayRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, ArrayWritable shapes) throws IOException {
    // Get cellInfo for the current position in file
    boolean element_read = nextShapes(shapes);
    key.set(cellMbr); // Set the cellInfo for the last block read
    return element_read;
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(shapeClass);
  }
  
}
