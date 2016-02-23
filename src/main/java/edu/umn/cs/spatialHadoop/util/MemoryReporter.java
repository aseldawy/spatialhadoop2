/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Runs a background daemon thread that keeps reporting the memory usage of
 * the JVM
 * @author Ahmed Eldawy
 *
 */
public class MemoryReporter implements Runnable {
  static final Log LOG = LogFactory.getLog(MemoryReporter.class);

  private String humanReadable(double size) {
    final String[] units = {"", "KB", "MB", "GB", "TB", "PB"};
    int unit = 0;
    while (unit < units.length && size > 1024) {
      size /= 1024;
      unit++;
    }
    return String.format("%.2f %s", size, units[unit]);
  }
  
  @Override
  public void run() {
    Runtime runtime = Runtime.getRuntime();
    while (true) {
      LOG.info(String.format("Free memory %s / Total memory %s",
          humanReadable(runtime.freeMemory()),
          humanReadable(runtime.totalMemory())));
      try {
        Thread.sleep(1000*60);
      } catch (InterruptedException e) {
      }
    }
  }
  
  public static Thread startReporting() {
    Thread thread = new Thread(new MemoryReporter(), "MemReporter");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }
}
