/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.util.Vector;

/**
 * SOme primitives to provide parallel processing over arrays and lists
 * @author Ahmed Eldawy
 *
 */
public class Parallel {

  private Parallel() { /* Static use only */ }
  
  public static interface RunnableRange<T> {
    public T run(int i1, int i2);
  }
  
  /**
   * An interface that is implemented by users to loop over a partial array.
   * @author Ahmed Eldawy
   *
   */
  public static class RunnableRangeThread<T> extends Thread {
    private int i1;
    private int i2;
    private RunnableRange<T> runnableRange;
    private T result;
    
    protected RunnableRangeThread(RunnableRange<T> runnableRange, int i1, int i2) {
      super("Worker ["+i1+","+i2+")");
      this.i1 = i1;
      this.i2 = i2;
      this.runnableRange = runnableRange;
    }
    
    @Override
    public void run() {
      this.result = this.runnableRange.run(i1, i2);
    }
    
    public T getResult() {
      return result;
    }
  }
  
  public static <T> Vector<T> forEach(int size, RunnableRange<T> r) {
    try {
      int parallelism = Math.min(size, Runtime.getRuntime().availableProcessors());
      final int[] partitions = new int[parallelism + 1];
      for (int i_thread = 0; i_thread <= parallelism; i_thread++)
        partitions[i_thread] = i_thread * size / parallelism;
      final Vector<RunnableRangeThread<T>> threads = new Vector<RunnableRangeThread<T>>();
      Vector<T> results = new Vector<T>();
      for (int i_thread = 0; i_thread < parallelism; i_thread++) {
        threads.add(new RunnableRangeThread<T>(r, partitions[i_thread], partitions[i_thread+1]));
        threads.lastElement().start();
      }
      for (int i_thread = 0; i_thread < parallelism; i_thread++) {
        threads.get(i_thread).join();
        results.add(threads.get(i_thread).getResult());
      }
      return results;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws InterruptedException {
    final int[] values = new int[1000000];
    for (int i = 0; i < values.length; i++)
      values[i] = i;
    Vector<Long> results = Parallel.forEach(values.length, new RunnableRange<Long>() {
      @Override
      public Long run(int i1, int i2) {
        long total = 0;
        for (int i = i1; i < i2; i++)
          total += values[i];
        return total;
      }
    });
    long finalResult = 0;
    for (Long result : results) {
      finalResult += result;
    }
    System.out.println(finalResult);
  }

}
