package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class TileIndexTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public TileIndexTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(TileIndexTest.class);
  }
  
  public void testGetMBR() {
    Rectangle spaceMBR = new Rectangle(0, 0, 1024, 1024);
    int x = 0, y = 0, z = 0;
    Rectangle tileMBR = TileIndex.getMBR(spaceMBR, z, x, y);
    Rectangle expectedMBR = spaceMBR;
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    z = 1;
    expectedMBR = new Rectangle(spaceMBR.x1, spaceMBR.y1,
        spaceMBR.getWidth() / 2, spaceMBR.getHeight() / 2);
    tileMBR = TileIndex.getMBR(spaceMBR, z, x, y);
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    z = 10;
    x = 100;
    y = 100;
    expectedMBR = new Rectangle(100, 100, 101, 101);
    tileMBR = TileIndex.getMBR(spaceMBR, z, x, y);
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));
  }

  public void testWithNonZeroOriginal() {
    Rectangle spaceMBR = new Rectangle(1024, 1024, 2048, 2048);
    long ti = TileIndex.encode(0, 0 ,0);
    Rectangle tileMBR = TileIndex.getMBR(spaceMBR, ti);
    Rectangle expectedMBR = spaceMBR;
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));
  }

  public void testEncodeAndDecode() {
    long ti = TileIndex.encode(6, 5, 7);
    assertEquals(0x60000500007L, ti);
    TileIndex tileIndex = null;
    tileIndex = TileIndex.decode(ti, tileIndex);
    assertEquals(6, tileIndex.z);
    assertEquals(5, tileIndex.x);
    assertEquals(7, tileIndex.y);
  }

}
