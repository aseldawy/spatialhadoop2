package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;

public class GridRecordWriter2<S extends Shape>
extends edu.umn.cs.spatialHadoop.core.GridRecordWriter<S> implements RecordWriter<NullWritable, S> {

  public GridRecordWriter2(JobConf job, String name, CellInfo[] cells, boolean pack, boolean expand) throws IOException {
    super(null, job, name, cells, pack, expand);
  }
  
  @Override
  public void write(NullWritable key, S value) throws IOException {
    super.write(key, value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
