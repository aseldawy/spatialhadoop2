package org.apache.hadoop.mapred.spatial;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;

/**
 * Reads a file stored as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeInputFormat<S extends Shape> extends SpatialInputFormat<CellInfo, RTree<S>> {

  @Override
	public RecordReader<CellInfo, RTree<S>> getRecordReader(InputSplit split,
	    JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    this.rrClass = RTreeRecordReader.class;
    return super.getRecordReader(split, job, reporter);
	}

}