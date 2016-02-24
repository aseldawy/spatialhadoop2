/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

package edu.umn.cs.spatialHadoop.nasa;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;


/**
 * Projects a NASAPoint in HDF file from Sinusoidal projection to Mercator
 * projection.
 * @author Ahmed Eldawy
 *
 */
public class MercatorProjector implements GeoProjector {
  
  @Override
  public void project(Shape shape) {
    if (shape instanceof Point) {
      Point pt = (Point) shape;
      // Use the Mercator projection to draw an image similar to Google Maps
      // http://stackoverflow.com/questions/14329691/covert-latitude-longitude-point-to-a-pixels-x-y-on-mercator-projection
      double latRad = pt.y * Math.PI / 180.0;
      double mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      pt.y = -180 * mercN / Math.PI;
    } else if (shape instanceof Rectangle) {
      Rectangle rect = (Rectangle) shape;
      double latRad = rect.y1 * Math.PI / 180.0;
      double mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      rect.y1 = -180 * mercN / Math.PI;
      
      latRad = rect.y2 * Math.PI / 180.0;
      mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      rect.y2 = -180 * mercN / Math.PI;
    } else {
      throw new RuntimeException("Cannot project shapes of type "+shape.getClass());
    }
  }
}
