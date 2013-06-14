package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

public class GridOutputFormat2<S extends Shape> extends FileOutputFormat<NullWritable, S> {

  @Override
  public RecordWriter<NullWritable, S> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Get grid info
    CellInfo[] cellsInfo = SpatialSite.getCells(job);
    boolean pack = job.getBoolean(SpatialSite.PACK_CELLS, false);
    GridRecordWriter2<S> writer = new GridRecordWriter2<S>(job, name, cellsInfo, pack);
    return writer;
  }
  
}

