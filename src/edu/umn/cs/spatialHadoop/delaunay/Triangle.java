/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

/**
 * A triangle that consists of three sites
 * @author Ahmed Eldawy
 *
 */
public class Triangle {
  /**The three sites of this triangle*/
  Site s1, s2, s3;
  
  Triangle(Site p1, Site p2, Site p3) {
    // Store the three sites lexicographically ordered by x and y
    // We assume that points are unique (no equal points)
    boolean p1ltp2 = p1.x <= p2.x && p1.y <= p2.y;
    boolean p1ltp3 = p1.x <= p3.x && p1.y <= p3.y;
    boolean p2ltp3 = p2.x <= p3.x && p2.y <= p3.y;
    
    if (p1ltp2) { // p1 < p2
      if (p1ltp3) { // p1 < p3 => p1 is the minimum
        // p1 is the minimum, now look for the second minimum in p2 and p3
        this.s1 = p1;
        if (p2ltp3) { // p2 < p3
          this.s2 = p2;
          this.s3 = p3;
        } else { // p3 < p2
          this.s2 = p3;
          this.s3 = p2;
        }
      } else { // p3 < p1 => p3 is the minimum (because p1 < p2)
        // p3 is the minimum, p1 is the second minimum (because p1 < p2)
        this.s1 = p3;
        this.s2 = p1;
        this.s3 = p2;
      }
    } else if (p2ltp3) { // p2 < p3 => p2 is the minimum (because p2 < p1)
      // p2 is the minimum, now look for the second minimum in p1 and p3
      this.s1 = p2;
      if (p1ltp3) { // p1 < p3
        this.s2 = p1;
        this.s3 = p3;
      } else { // p3 < p1
        this.s2 = p3;
        this.s3 = p1;
      }
    }
  }
  
  @Override
  public int hashCode() {
    return s1.hashCode() + s2.hashCode() + s3.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    Triangle t2 = (Triangle) obj;
    return this.s1 == t2.s1 && this.s2 == t2.s3 && this.s3 == t2.s3;
  }
}
