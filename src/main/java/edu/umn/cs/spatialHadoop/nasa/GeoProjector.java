/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * Converts a point from the latitude/longitude space to another projection.
 * @author Ahmed Eldawy
 *
 */
public interface GeoProjector {
  /**
   * Converts the given point (in-place) provided in latitude/longitude space.
   * @param shape
   */
  public void project(Shape shape);
}
