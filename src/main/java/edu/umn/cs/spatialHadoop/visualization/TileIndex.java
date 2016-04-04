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

import org.apache.hadoop.io.WritableComparable;

/**
 * An class that represents a position of a tile in the pyramid.
 * Level is the level of the tile starting with 0 at the top.
 * x and y are the index of the column and row of the tile in the grid
 * at this level.
 * @author Ahmed Eldawy
 *
 */
public class TileIndex implements WritableComparable<TileIndex> {
  public int level, x, y;
  
  public TileIndex() {}
  
  public TileIndex(int level, int x, int y) {
    super();
    this.level = level;
    this.x = x;
    this.y = y;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    level = in.readInt();
    x = in.readInt();
    y = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(level);
    out.writeInt(x);
    out.writeInt(y);
  }

  @Override
  public int compareTo(TileIndex a) {
    if (this.level != a.level)
      return this.level - a.level;
    if (this.x != a.x)
      return this.x - a.x;
    return this.y - a.y;
  }
  
  @Override
  public String toString() {
    return "Level: "+level+" @("+x+","+y+")";
  }
  
  @Override
  public int hashCode() {
    return level * 31 + x * 25423 + y;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    TileIndex b = (TileIndex) obj;
    return this.level == b.level && this.x == b.x && this.y == b.y;
  }
  
  @Override
  public TileIndex clone() {
    return new TileIndex(this.level, this.x, this.y);
  }

  public String getImageFileName() {
    return "tile-"+this.level+"-"+this.x+"-"+this.y;
  }
}
