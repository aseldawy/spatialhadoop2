package org.apache.hadoop.mapred.spatial;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;

/**
 * Reads a file stored as a list of RTrees
 * @author eldawy
 *
 */
public class ShapeArrayInputFormat extends SpatialInputFormat<CellInfo, ArrayWritable> {

	@Override
	public RecordReader<CellInfo, ArrayWritable> getRecordReader(InputSplit split,
	    JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    this.rrClass = ShapeArrayRecordReader.class;
    return super.getRecordReader(split, job, reporter);
	}
}