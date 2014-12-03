/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.core.ResultCollector;

/**
 * Builds a wrapper around an existing ResultCollector which
 * synchronizes all calls to the wrapped ResultCollector.
 * @author Ahmed Eldawy
 *
 */
public class ResultCollectorSynchronizer<T> implements ResultCollector<T> {
  
  private ResultCollector<T> wrapped;

  public ResultCollectorSynchronizer(ResultCollector<T> wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public synchronized void collect(T t) {
    wrapped.collect(t);
  }

}
