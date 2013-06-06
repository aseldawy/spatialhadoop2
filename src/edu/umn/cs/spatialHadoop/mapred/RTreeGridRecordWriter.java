package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;

public class RTreeGridRecordWriter<S extends Shape>
    extends edu.umn.cs.spatialHadoop.core.RTreeGridRecordWriter<S>
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
