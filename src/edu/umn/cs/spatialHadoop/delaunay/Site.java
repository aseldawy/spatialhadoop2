/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A site in Delaunay triangulation
 * @author Ahmed Eldawy
 *
 */
public class Site {
  int id;
  
  /**All neighboring sites. A neighbor site has a common edge in the 
   * Delaunay triangulation*/
  IntArray neighbors = new IntArray();
  
  Site(int id) {
    this.id = id;
  }
}
