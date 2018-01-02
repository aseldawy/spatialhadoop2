package edu.umn.cs.spatialHadoop.core;

import edu.umn.cs.spatialHadoop.operations.Head;
import edu.umn.cs.spatialHadoop.util.FSUtil;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Unit test for the utility class {@link Head}.
 */
public class GridInfoTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public GridInfoTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(GridInfoTest.class);
  }

  public void testGetOverlappingCells() {
    GridInfo gridInfo = new GridInfo(0, 0, 16, 16, 16, 16);
    java.awt.Rectangle overlappingCells = gridInfo.getOverlappingCells(new Rectangle(0.5, 0.5, 1.5, 1.5));
    assertEquals(0, overlappingCells.x);
    assertEquals(0, overlappingCells.y);
    assertEquals(2, overlappingCells.width);
    assertEquals(2, overlappingCells.height);

    overlappingCells = gridInfo.getOverlappingCells(new Rectangle(0.5, 0.5, 1.0, 1.0));
    assertEquals(0, overlappingCells.x);
    assertEquals(0, overlappingCells.y);
    assertEquals(1, overlappingCells.width);
    assertEquals(1, overlappingCells.height);

  }

}
