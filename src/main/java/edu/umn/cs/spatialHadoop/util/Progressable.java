/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.IOException;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An interface used to report the progress of lengthy algorithms in
 * SpatialHaodop.
 * @author Ahmed Eldawy
 *
 */
public interface Progressable extends org.apache.hadoop.util.Progressable {
  
  /**
   * A null progressable that skips all calls. Can be used as a base class if
   * not all methods need to be implemented.
   * @author Ahmed Eldawy
   *
   */
  public static class NullProgressable implements Progressable {
    @Override
    public void progress() {}
    @Override
    public void progress(float p) { progress(); }
    @Override
    public void setStatus(String status) { progress(); }
  }
  
  /**
   * A wrapper around a mapper or reducer context.
   * @author Ahmed Eldawy
   *
   */
  public static class TaskProgressable implements Progressable {
    private TaskAttemptContext context;
    private int lastProgress = 0;
    private String status = "";

    public TaskProgressable(TaskAttemptContext context){
      this.context = context;
    }

    @Override
    public void progress() {
      this.context.progress();
    }

    @Override
    public void progress(float p) throws IOException {
      int prog = (int) (p * 100);
      if (prog > lastProgress) {
        lastProgress = prog;
        context.setStatus(this.status+" ("+lastProgress+"%)");
      }
    }

    @Override
    public void setStatus(String status) throws IOException {
      this.status = status;
      this.context.setStatus(this.status+" ("+lastProgress+"%)");
    }
  }
  
  public static class ReporterProgressable implements Progressable {
    
    /**Underlying reporter to update*/
    private Reporter reporter;
    
    private int lastProgress = 0;
    private String status = "";
    
    public ReporterProgressable(Reporter reporter) {
      super();
      this.reporter = reporter;
    }
    
    @Override
    public void progress() {
      this.reporter.progress();
    }

    @Override
    public void progress(float p) throws IOException {
      int prog = (int) (p * 100);
      if (prog > lastProgress) {
        lastProgress = prog;
        reporter.setStatus(this.status+" ("+lastProgress+"%)");
      }
    }

    @Override
    public void setStatus(String status) throws IOException {
      this.status = status;
      this.reporter.setStatus(this.status+" ("+lastProgress+"%)");
    }
    
  }
  
  /**
   * Reports that the algorithm is making progress. It did not get stuck.
   */
  void progress();
  
  /**
   * The algorithms reports its progress as a float in the range [0, 1], where
   * 0.0 means no progress at all and 1.0 means the algorithm has finished.
   * @param p
   */
  void progress(float p) throws IOException;
  
  /**
   * The algorithm reports its current status. Multi-phase algorithms can use
   * this function to reports its current phase.
   * @param status
   */
  void setStatus(String status) throws IOException;
}
