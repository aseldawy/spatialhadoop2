package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

public class RTreeGridOutputFormat<S extends Shape> extends FileOutputFormat<IntWritable, S> {

  @Override
  public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Get grid info
    CellInfo[] cellsInfo = SpatialSite.getCells(job);
    boolean pack = job.getBoolean(SpatialSite.PACK_CELLS, false);
    boolean expand = job.getBoolean(SpatialSite.EXPAND_CELLS, false);
    RTreeGridRecordWriter<S> writer = new RTreeGridRecordWriter<S>(job, name, cellsInfo, pack, expand);
    writer.setStockObject((S) SpatialSite.createStockShape(job));
    return writer;
  }

}

