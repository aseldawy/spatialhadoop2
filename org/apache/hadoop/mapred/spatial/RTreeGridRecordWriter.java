package org.apache.hadoop.mapred.spatial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;

public class RTreeGridRecordWriter<S extends Shape>
    extends org.apache.hadoop.spatial.RTreeGridRecordWriter<S>
    implements RecordWriter<IntWritable, S> {

  public RTreeGridRecordWriter(JobConf job, String prefix, CellInfo[] cells, boolean pack) throws IOException {
    super(null, job, prefix, cells, pack);
  }
  
  @Override
  public void write(IntWritable key, S value) throws IOException {
    super.write(key.get(), value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
