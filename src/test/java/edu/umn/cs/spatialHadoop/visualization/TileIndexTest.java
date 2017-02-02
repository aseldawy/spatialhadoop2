package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

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
    TileIndex ti = new TileIndex(0, 0 ,0);
    Rectangle tileMBR = ti.getMBR(spaceMBR);
    Rectangle expectedMBR = spaceMBR;
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    ti.level = 1;
    expectedMBR = new Rectangle(spaceMBR.x1, spaceMBR.y1,
        spaceMBR.getWidth() / 2, spaceMBR.getHeight() / 2);
    tileMBR = ti.getMBR(spaceMBR);
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    ti.level = 10;
    ti.x = 100;
    ti.y = 100;
    expectedMBR = new Rectangle(100, 100, 101, 101);
    tileMBR = ti.getMBR(spaceMBR);
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));
  }

  public void testWithNonZeroOriginal() {
    Rectangle spaceMBR = new Rectangle(1024, 1024, 2048, 2048);
    TileIndex ti = new TileIndex(0, 0 ,0);
    Rectangle tileMBR = ti.getMBR(spaceMBR);
    Rectangle expectedMBR = spaceMBR;
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));
  }

}
