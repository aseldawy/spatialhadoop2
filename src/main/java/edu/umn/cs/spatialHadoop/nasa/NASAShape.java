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
 * A shape coming from NASA datasets. It contains an extra value corresponding
 * to the physical reading of the underlying area.
 * @author Ahmed Eldawy
 *
 */
public interface NASAShape extends Shape {
  public void setValue(int v);
  public int getValue();
  
  public void setTimestamp(long t);
  public long getTimestamp();
}
