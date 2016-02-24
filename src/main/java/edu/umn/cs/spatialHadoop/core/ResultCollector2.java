/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

/**
 * Used to collect the output of binary operators.
 * @author eldawy
 *
 */
public interface ResultCollector2<R, S> {
  public void collect(R r, S s);
}
