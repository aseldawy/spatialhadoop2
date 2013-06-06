package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
    CellInfo[] cellsInfo = decodeCells(job.get(OUTPUT_CELLS));
    boolean pack = job.getBoolean(SpatialSite.PACK_CELLS, false);
    GridRecordWriter<S> writer = new GridRecordWriter<S>(job, name, cellsInfo, pack);
    return writer;
  }
  
  public static String encodeCells(CellInfo[] cellsInfo) {
    String encodedCellsInfo = "";
    for (CellInfo cellInfo : cellsInfo) {
      if (encodedCellsInfo.length() > 0)
        encodedCellsInfo += ";";
      Text text = new Text();
      cellInfo.toText(text);
      encodedCellsInfo += text.toString();
    }
    return encodedCellsInfo;
  }
  
  public static CellInfo[] decodeCells(String encodedCells) {
    String[] parts = encodedCells.split(";");
    CellInfo[] cellsInfo = new CellInfo[parts.length];
    for (int i = 0; i < parts.length; i++) {
      cellsInfo[i] = new CellInfo(parts[i]);
    }
    return cellsInfo;
  }
}

