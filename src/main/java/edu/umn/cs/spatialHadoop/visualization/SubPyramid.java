package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * Represents a part of the pyramid structure defined by a minimum z,
 * a maximum z, and a rectangular range of tiles at the maximum z.
 * Created by Ahmed Eldawy on 1/2/18.
 */
public class SubPyramid {
  /**
   * The MBR covered by the full pyramid (not this subpyramid).
   * The range is inclusive for (x1, y1) and exclusive for (x2, y2)
   */
  public double x1, y1, x2, y2;

  /** The range of levels (inclusive) covered by the subpyramid */
  public int minimumLevel, maximumLevel;

  /**
   * The range of tiles covered by this subpyramid at the maximumLevel
   * The range includes the first row (r1), the first column (c1), and
   * excludes the last row (r2) and last column (c2).
   */
  public int c1, r1, c2, r2;

  /** The width and height of each tile at maximumLevel */
  private double tileWidth, tileHeight;

  public SubPyramid(Rectangle mbr, int minLevel, int maxLevel,
                    int c1, int r1, int c2, int r2) {
    this.set(mbr, minLevel, maxLevel, c1, r1, c2, r2);
  }

  public SubPyramid() {
  }

  /**
   *
   * @param mbr
   * @param minLevel
   * @param maxLevel
   * @param c1
   * @param r1
   * @param c2
   * @param r2
   */
  public void set(Rectangle mbr, int minLevel, int maxLevel,
                  int c1, int r1, int c2, int r2) {
    this.x1 = mbr.x1;
    this.x2 = mbr.x2;
    this.y1 = mbr.y1;
    this.y2 = mbr.y2;
    this.minimumLevel = minLevel;
    this.maximumLevel = maxLevel;
    this.c1 = c1;
    this.r1 = r1;
    this.c2 = c2;
    this.r2 = r2;
    this.tileWidth = (x2 - x1) / (1 << maximumLevel);
    this.tileHeight = (y2 - y1) / (1 << maximumLevel);
  }

  /**
   * Set the sub-pyramid to cover only one tile
   * @param mbr The MBR of the entire data set
   * @param z The level of the given tile starting at zero
   * @param c The column of the given tile
   * @param r The row of the given tile
   */
  public void set(Rectangle mbr, int z, int c, int r) {
    this.x1 = mbr.x1;
    this.x2 = mbr.x2;
    this.y1 = mbr.y1;
    this.y2 = mbr.y2;
    this.minimumLevel = this.maximumLevel = z;
    this.c1 = c;
    this.c2 = c + 1;
    this.r1 = r;
    this.r2 = r + 1;
    this.tileWidth = (x2 - x1) / (1 << maximumLevel);
    this.tileHeight = (y2 - y1) / (1 << maximumLevel);
  }

  /**
   * Returns the range of tiles that overlaps the given rectangle at
   * the maximum z
   * @param rect
   * @param overlaps
   */
  public void getOverlappingTiles(Rectangle rect, java.awt.Rectangle overlaps) {
    overlaps.x = (int) Math.floor((rect.x1 - this.x1) / tileWidth);
    if (overlaps.x < c1)
      overlaps.x = c1;
    overlaps.y = (int) Math.floor((rect.y1 - this.y1) / tileHeight);
    if (overlaps.y < r1)
      overlaps.y = r1;
    overlaps.width = (int) Math.ceil((rect.x2 - this.x1) / tileWidth) - overlaps.x;
    if (overlaps.x + overlaps.width > c2)
      overlaps.width = c2 - overlaps.x;
    overlaps.height = (int) Math.ceil((rect.y2 - this.y1) / tileHeight) - overlaps.y;
    if (overlaps.y + overlaps.height > r2)
      overlaps.height = r2 - overlaps.y;
  }

  public Rectangle getInputMBR() {
    return new Rectangle(x1, y1, x2, y2);
  }
}
