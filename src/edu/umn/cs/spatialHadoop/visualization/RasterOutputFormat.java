/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Writes raster layers to a binary output file
 * @author Ahmed Eldawy
 *
 */
public class RasterOutputFormat extends FileOutputFormat<NullWritable, RasterLayer> {

  /**
   * Writes raster layers to a file
   * @author Ahmed Eldawy
   *
   */
  class RasterRecordWriter implements RecordWriter<NullWritable, RasterLayer> {
    /**Progress of the output format*/
    private Progressable progress;
    /**The output file where all raster layers are written*/
    private FSDataOutputStream outFile;

    public RasterRecordWriter(FileSystem fs, Path taskOutputPath, JobConf job,
        Progressable progress) throws IOException {
      this.progress = progress;
      this.outFile = fs.create(taskOutputPath);
    }

    @Override
    public void write(NullWritable dummy, RasterLayer r) throws IOException {
      r.write(outFile);
      progress.progress();
    }
    
    @Override
    public void close(Reporter reporter) throws IOException {
      outFile.close();
    }
  }
  
  @Override
  public RecordWriter<NullWritable, RasterLayer> getRecordWriter(
      FileSystem fs, JobConf job, String name, Progressable progress)
      throws IOException {
    Path taskOutputPath = getTaskOutputPath(job, name);
    return new RasterRecordWriter(fs, taskOutputPath, job, progress);
  }

}
