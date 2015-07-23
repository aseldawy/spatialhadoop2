/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

/**
 * Some utilities for computing the Delaunay triangulation
 * @author Ahmed Eldawy
 *
 */
public final class Utils {
  static boolean inArray(Object[] array, Object objectToFind) {
    for (Object objectToCompare : array)
      if (objectToFind == objectToCompare)
        return true;
    return false;
  }
  
  static double calculateCWAngle(Site s1, Site s2, Site s3) {
    double angle1 = Math.atan2(s1.y - s2.y, s1.x - s2.x);
    double angle2 = Math.atan2(s3.y - s2.y, s3.x - s2.x);
    return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
  }

  static Site calculateCircumCircleCenter(Site s1, Site s2, Site s3) {
    // Calculate the perpendicular bisector of the first two points
    double x1 = (s1.x + s2.x) / 2;
    double y1 = (s1.y + s2.y) /2;
    double x2 = x1 + s2.y - s1.y;
    double y2 = y1 + s1.x - s2.x;
    // Calculate the perpendicular bisector of the second two points 
    double x3 = (s3.x + s2.x) / 2;
    double y3 = (s3.y + s2.y) / 2;
    double x4 = x3 + s2.y - s3.y;
    double y4 = y3 + s3.x - s2.x;
    
    // Calculate the intersection of the two new lines
    // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
    double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
    double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
    return new Site(ix, iy);
  }
  
  static Site[] convexHull(Site[] points) {
    Stack<Site> lowerChain = new Stack<Site>();
    Stack<Site> upperChain = new Stack<Site>();

    // Sort sites by increasing x-axis
    Arrays.sort(points, new Comparator<Site>() {
      @Override
      public int compare(Site s1, Site s2) {
        if (s1.x < s2.x)
          return -1;
        if (s1.x > s2.x)
          return +1;
        if (s1.y < s2.y)
          return -1;
        if (s1.y > s2.y)
          return +1;
        return 0;
      }
    });
    
    // Lower chain
    for (int i=0; i<points.length; i++) {
      while(lowerChain.size() > 1) {
        Site s1 = lowerChain.get(lowerChain.size() - 2);
        Site s2 = lowerChain.get(lowerChain.size() - 1);
        Site s3 = points[i];
        double crossProduct = (s2.x - s1.x) * (s3.y - s1.y) - (s2.y - s1.y) * (s3.x - s1.x);
        if (crossProduct <= 0) lowerChain.pop();
        else break;
      }
      lowerChain.push(points[i]);
    }
    
    // Upper chain
    for (int i=points.length - 1; i>=0; i--) {
      while(upperChain.size() > 1) {
        Site s1 = upperChain.get(upperChain.size() - 2);
        Site s2 = upperChain.get(upperChain.size() - 1);
        Site s3 = points[i];
        double crossProduct = (s2.x - s1.x) * (s3.y - s1.y) - (s2.y - s1.y) * (s3.x - s1.x);
        if (crossProduct <= 0) upperChain.pop();
        else break;
      }
      upperChain.push(points[i]);
    }
    
    lowerChain.pop();
    upperChain.pop();
    lowerChain.addAll(upperChain);
    return lowerChain.toArray(new Site[lowerChain.size()]);    
  }
}
