package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;


/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * @author eldawy
 *
 * @param <S>
 */
public class ShapeInputFormat<S extends Shape> extends SpatialInputFormat<Rectangle, S> {
  
  @Override
  public RecordReader<Rectangle, S> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    this.rrClass = ShapeRecordReader.class;
    return super.getRecordReader(split, job, reporter);
  }
}
