package edu.umn.cs.spatialHadoop.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

public class GridOutputFormat<S extends Shape> extends FileOutputFormat<IntWritable, S> {
  public static final String OUTPUT_CELLS = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.CellsInfo";
  public static final String OVERWRITE = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.Overwrite";
  public static final String RTREE = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.RTree";

  @Override
  public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Get grid info
    CellInfo[] cellsInfo = getCells(job);
    boolean pack = job.getBoolean(SpatialSite.PACK_CELLS, false);
    GridRecordWriter<S> writer = new GridRecordWriter<S>(job, name, cellsInfo, pack);
    return writer;
  }
  
  public static void setCells(JobConf job, CellInfo[] cellsInfo) throws IOException {
    File tempFile = File.createTempFile(job.getJobName(), "cells");
    FSDataOutputStream out = FileSystem.getLocal(job).create(new Path(tempFile.getPath()));
    out.writeInt(cellsInfo.length);
    for (CellInfo cell : cellsInfo) {
      cell.write(out);
    }
    out.close();

    DistributedCache.addCacheFile(tempFile.toURI(), job);
    job.set(OUTPUT_CELLS, tempFile.getName());
  }
  
  public static CellInfo[] getCells(JobConf job) throws IOException {
    CellInfo[] cells = null;
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
    String cells_file = job.get(OUTPUT_CELLS);
    for (Path cacheFile : cacheFiles) {
      if (cacheFile.getName().equals(cells_file)) {
        FSDataInputStream in = FileSystem.getLocal(job).open(cacheFile);
        
        int cellCount = in.readInt();
        cells = new CellInfo[cellCount];
        for (int i = 0; i < cellCount; i++) {
          cells[i] = new CellInfo();
          cells[i].readFields(in);
        }
        
        in.close();
      }
    }
    return cells;
  }
}

