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
 * Level is the level of the tile starting with 0 at the top.
 * x and y are the index of the column and row of the tile in the grid
 * at this level.
 * @author Ahmed Eldawy
 *
 */
public class TileIndex {
  /**Number of bits to reserve for each coordinate, x and y */
  private static final int CoordinateBits = 20;
  private static final int CoordinateMask = 0xfffff;
  /**Number of bits to reserve for the level, z*/
  private static final int LevelBits = 6;

  /**Coordinates of the tile*/
  public int z, x, y;
  
  private TileIndex() {}

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

  public static long encode(int z, int x, int y) {
    return (((long)z) << (CoordinateBits * 2)) | (((long)x) << CoordinateBits) | y;
  }

  public static TileIndex decode(long encodedTileID, TileIndex tileIndex) {
    if (tileIndex == null)
      tileIndex = new TileIndex();
    tileIndex.z = (int) (encodedTileID >> (CoordinateBits * 2));
    tileIndex.x = (int) ((encodedTileID >> CoordinateBits) & CoordinateMask);
    tileIndex.y = (int) (encodedTileID & CoordinateMask);
    return tileIndex;
  }
  
  /**
   * Returns the MBR of this rectangle given that the MBR of whole pyramid
   * (i.e., the top tile) is the given one.
   * @param spaceMBR
   * @return
   */
  public static Rectangle getMBR(Rectangle spaceMBR, int z, int x, int y) {
    int fraction = 1 << z;
    double tileWidth = spaceMBR.getWidth() / fraction;
    double tileHeight = spaceMBR.getHeight() / fraction;
    double x1 = spaceMBR.x1 + tileWidth * x;
    double y1 = spaceMBR.y1 + tileWidth * y;
    // TODO reuse a rectangle object to avoid creating too many objects
    return new Rectangle(x1, y1, x1 + tileWidth, y1 + tileHeight);
  }

  public static Rectangle getMBR(Rectangle spaceMBR, long encodedTileID) {
    int z = (int) (encodedTileID >> (CoordinateBits * 2));
    int x = (int) ((encodedTileID >> CoordinateBits) & CoordinateMask);
    int y = (int) (encodedTileID & CoordinateMask);
    return getMBR(spaceMBR, z, x, y);
  }

  public void moveToParent() {
    this.x /= 2;
    this.y /= 2;
    this.z--;
  }
}
