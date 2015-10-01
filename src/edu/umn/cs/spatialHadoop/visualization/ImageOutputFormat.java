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
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.Text2;

/**
 * Writes canvases as images to the output file
 * @author Ahmed Eldawy
 *
 */
public class ImageOutputFormat extends FileOutputFormat<Object, Canvas> {

  /**
   * Writes canvases to a file
   * @author Ahmed Eldawy
   *
   */
  class ImageRecordWriter extends RecordWriter<Object, Canvas> {
    /**Plotter used to merge intermediate canvases*/
    private Plotter plotter;
    private Path outPath;
    private FileSystem outFS;
    private int canvasesWritten;
    private boolean vflip;
    /**The associated reduce task. Used to report progress*/
    private TaskAttemptContext task;
    /**PrintStream to write the master file*/
    private PrintStream masterFile;
    /**The canvas resulting of merging all written canvases*/
    private Canvas mergedCanvas;
    private int imageHeight;

    public ImageRecordWriter(FileSystem fs, Path taskOutputPath,
        TaskAttemptContext task) throws IOException {
      Configuration conf = task.getConfiguration();
      this.task = task;
      this.plotter = Plotter.getPlotter(conf);
      this.outPath = taskOutputPath;
      this.outFS = this.outPath.getFileSystem(conf);
      this.canvasesWritten = 0;
      this.vflip = conf.getBoolean("vflip", true);
      // Create a canvas that should have been used to merge all images
      // This is used to calculate the position of each intermediate image
      // in the final image space to write the master file
      int imageWidth = conf.getInt("width", 1000);
      imageHeight = conf.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
      this.mergedCanvas = plotter.createCanvas(imageWidth, imageHeight, inputMBR);
      // Create a master file that logs the location of each intermediate image
      String masterFileName = String.format("_master-%05d.heap", task.getTaskAttemptID().getTaskID().getId());
      Path masterFilePath = new Path(outPath.getParent(), masterFileName);
      this.masterFile = new PrintStream(outFS.create(masterFilePath));
    }

    @Override
    public void write(Object dummy, Canvas r) throws IOException {
      String suffix = String.format("-%05d.png", canvasesWritten++);
      Path p = new Path(outPath.getParent(), outPath.getName()+suffix);
      FSDataOutputStream outFile = outFS.create(p);
      // Write the merged canvas
      plotter.writeImage(r, outFile, this.vflip);
      outFile.close();
      task.progress();
      
      java.awt.Point imageLocation = mergedCanvas.projectToImageSpace(r.inputMBR.x1, r.inputMBR.y2);
      masterFile.printf("%d,%d,%s\n", imageLocation.x, imageHeight - imageLocation.y, p.getName());
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      masterFile.close();
    }
  }
  
  @Override
  public RecordWriter<Object, Canvas> getRecordWriter(
      TaskAttemptContext task) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "");
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new ImageRecordWriter(fs, file, task);
  }
  
  /**
   * An output committer that merges all master files into one master file.
   * @author Ahmed Eldawy
   *
   */
  public static class MasterMerger extends FileOutputCommitter {
    /**Job output path*/
    private Path outPath;

    public MasterMerger(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      FileSystem fs = outPath.getFileSystem(context.getConfiguration());
      FileStatus[] masterFiles = fs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return p.getName().startsWith("_master");
        }
      });
      
      Path mergedFilePath = new Path(outPath, "_master.heap");
      PrintStream mergedFile = new PrintStream(fs.create(mergedFilePath));
      Path htmlPath = new Path(outPath, "_master.html");
      PrintStream htmlFile = new PrintStream(fs.create(htmlPath));
      htmlFile.print("<html> <body>");
      Text line = new Text2();
      for (FileStatus masterFile : masterFiles) {
        LineReader reader = new LineReader(fs.open(masterFile.getPath()));
        while (reader.readLine(line) > 0) {
          mergedFile.println(line);
          String[] parts = line.toString().split(",");
          htmlFile.printf("<img src='%s' style='position: absolute; left: %s; top: %s; border: dotted 1px black;'/>\n", parts[2], parts[0], parts[1]);
        }
        reader.close();
        
        fs.delete(masterFile.getPath(), false);
      }
      htmlFile.print("</body> </html>");
      mergedFile.close();
      htmlFile.close();
    }
  }
  
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    Path jobOutputPath = getOutputPath(context);
    return new MasterMerger(jobOutputPath, context);
  }

}
