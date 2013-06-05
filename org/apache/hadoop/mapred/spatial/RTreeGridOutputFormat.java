package org.apache.hadoop.mapred.spatial;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.Progressable;

public class RTreeGridOutputFormat<S extends Shape> extends FileOutputFormat<IntWritable, S> {

  @Override
  public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Get grid info
    CellInfo[] cellsInfo = GridOutputFormat.decodeCells(job.get(GridOutputFormat.OUTPUT_CELLS));
    boolean pack = job.getBoolean(SpatialSite.PACK_CELLS, false);
    RTreeGridRecordWriter<S> writer = new RTreeGridRecordWriter<S>(job, name, cellsInfo, pack);
    writer.setStockObject((S) SpatialSite.createStockShape(job));
    return writer;
  }

}

