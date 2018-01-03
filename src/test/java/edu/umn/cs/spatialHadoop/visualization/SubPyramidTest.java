package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class SubPyramidTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public SubPyramidTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(SubPyramidTest.class);
  }
  
  public void testOverlappingCells() {
    Rectangle mbr = new Rectangle(0,0,10,10);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 1, 0,0,2,2);

    java.awt.Rectangle overlaps = new java.awt.Rectangle();
    subPyramid.getOverlappingTiles(new Rectangle(5,5,10,10), overlaps);
    assertEquals(1, overlaps.x);
    assertEquals(1, overlaps.y);
    assertEquals(1, overlaps.width);
    assertEquals(1, overlaps.height);
  }

  public void testOverlappingCellsShouldClip() {
    Rectangle mbr = new Rectangle(0,0,1024,1024);
    SubPyramid subPyramid = new SubPyramid(mbr, 4, 6, 16,8,20,12);
    java.awt.Rectangle overlaps = new java.awt.Rectangle();

    // Case 1: No clipping required
    subPyramid.getOverlappingTiles(new Rectangle(256,128,320,192), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 2: Clip part on the top left
    subPyramid.getOverlappingTiles(new Rectangle(128,64,320,192), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 3: Clip part on the bottom right
    subPyramid.getOverlappingTiles(new Rectangle(256,128,512,256), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 4: If completely outside, return an empty rectangle with non-positive width or height
    subPyramid.getOverlappingTiles(new Rectangle(128,64,196,96), overlaps);
    assertTrue(overlaps.width <= 0 || overlaps.height <= 0);
  }

  public void testSetFromTileID() {
    Rectangle mbr = new Rectangle(0,0,1024,1024);
    SubPyramid subPyramid = new SubPyramid();
    subPyramid.set(mbr, 3, 1, 3);

    assertEquals(3, subPyramid.minimumLevel);
    assertEquals(3, subPyramid.maximumLevel);
    assertEquals(1, subPyramid.c1);
    assertEquals(2, subPyramid.c2);
    assertEquals(3, subPyramid.r1);
    assertEquals(4, subPyramid.r2);
  }

}
