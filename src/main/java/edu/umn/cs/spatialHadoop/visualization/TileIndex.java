/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import org.apache.hadoop.io.WritableComparable;

/**
 * An class that represents a position of a tile in the pyramid.
 * Level is the z of the tile starting with 0 at the top.
 * x and y are the index of the column and row of the tile in the grid
 * at this z.
 * @author Ahmed Eldawy
 *
 */
public class TileIndex implements WritableComparable<TileIndex> {
  public int z, x, y;
  
  public TileIndex() {}
  
  public TileIndex(int z, int x, int y) {
    super();
    this.z = z;
    this.x = x;
    this.y = y;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    z = in.readInt();
    x = in.readInt();
    y = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(z);
    out.writeInt(x);
    out.writeInt(y);
  }

  @Override
  public int compareTo(TileIndex a) {
    if (this.z != a.z)
      return this.z - a.z;
    if (this.x != a.x)
      return this.x - a.x;
    return this.y - a.y;
  }
  
  @Override
  public String toString() {
    return "Level: "+ z +" @("+x+","+y+")";
  }
  
  @Override
  public int hashCode() {
    return z * 31 + x * 25423 + y;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    TileIndex b = (TileIndex) obj;
    return this.z == b.z && this.x == b.x && this.y == b.y;
  }
  
  @Override
  public TileIndex clone() {
    return new TileIndex(this.z, this.x, this.y);
  }

  public String getImageFileName() {
    return "tile-"+this.z +"-"+this.x+"-"+this.y;
  }

  /**
   * Returns the MBR of this rectangle given that the MBR of whole pyramid
   * (i.e., the top tile) is the given one.
   * @param spaceMBR
   * @return
   */
  public Rectangle getMBR(Rectangle spaceMBR) {
    int fraction = 1 << this.z;
    double tileWidth = spaceMBR.getWidth() / fraction;
    double tileHeight = spaceMBR.getHeight() / fraction;
    double x1 = spaceMBR.x1 + tileWidth * this.x;
    double y1 = spaceMBR.y1 + tileWidth * this.y;
    return new Rectangle(x1, y1, x1 + tileWidth, y1 + tileHeight);
  }
  
  public void moveToParent() {
	this.x /= 2;
	this.y /= 2;
	this.z--;
  }
}
