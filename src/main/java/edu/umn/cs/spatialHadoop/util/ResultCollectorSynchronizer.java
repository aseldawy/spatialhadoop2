/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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
