package org.apache.hadoop.mapred.spatial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;

public class GridRecordWriter<S extends Shape>
extends org.apache.hadoop.spatial.GridRecordWriter<S> implements RecordWriter<IntWritable, S> {

  public GridRecordWriter(JobConf job, String name, CellInfo[] cells, boolean pack) throws IOException {
    super(null, job, name, cells, pack);
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
