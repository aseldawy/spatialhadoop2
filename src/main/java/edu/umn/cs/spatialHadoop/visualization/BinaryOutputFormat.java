/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Writes canvases as images to the output file
 * @author Ahmed Eldawy
 *
 */
public class BinaryOutputFormat extends FileOutputFormat<Writable, Writable> {

  /**
   * Writes canvases to a file
   * @author Ahmed Eldawy
   *
   */
  class BinaryRecordWriter extends RecordWriter<Writable, Writable> {
    /**Plotter used to merge intermediate canvases*/
    private FSDataOutputStream out;

    public BinaryRecordWriter(FSDataOutputStream out) throws IOException {
      this.out = out;
    }


    @Override
    public void write(Writable key, Writable value) throws IOException {
    	key.write(out);
    	value.write(out);
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
    	out.close();
    }
  }
  
  @Override
  public RecordWriter<Writable, Writable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
	  Configuration conf = job.getConfiguration();
	  Path file = getDefaultWorkFile(job, "");
	  FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, false);
      return new BinaryRecordWriter(fileOut);
  }
  

}
