package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Unit test for the RTreeGuttman class
 */
public class RTreeGuttmanTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public RTreeGuttmanTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(RTreeGuttmanTest.class);
  }

  public void testBuild() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 4, 8);
      assertEquals(rtree.numOfDataEntries(), 11);
      assertEquals(3, rtree.numOfNodes());
      assertEquals(1, rtree.getHeight());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testBuild2() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 4, 8);
      assertEquals(rtree.numOfDataEntries(), 22);
      int maxNumOfNodes = 6;
      int minNumOfNodes = 4;
      assertTrue(String.format("Too few nodes %d<%d",rtree.numOfNodes(), minNumOfNodes),
          rtree.numOfNodes() >= minNumOfNodes);
      assertTrue(String.format("Too many nodes %d>%d", rtree.numOfNodes(), maxNumOfNodes),
          rtree.numOfNodes() <= maxNumOfNodes);
      assertEquals(1, rtree.getHeight());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testSplit() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      // Create a tree with a very large threshold to avoid any splits
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 50, 100);
      assertEquals(11, rtree.numOfDataEntries());
      // Make one split at the root
      int iNode = rtree.iRoot;
      int iNewNode = rtree.split(iNode, 4);
      assertTrue("Too small size " + rtree.Node_size(iNewNode), rtree.Node_size(iNewNode) > 2);
      assertTrue("Too small size " + rtree.Node_size(iNode), rtree.Node_size(iNode) > 2);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }


  public void testBuild3() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 2, 4);
      assertEquals(rtree.numOfDataEntries(), 22);
      int maxNumOfNodes = 18;
      int minNumOfNodes = 9;
      assertTrue(String.format("Too few nodes %d<%d",rtree.numOfNodes(), minNumOfNodes),
          rtree.numOfNodes() >= minNumOfNodes);
      assertTrue(String.format("Too many nodes %d>%d", rtree.numOfNodes(), maxNumOfNodes),
          rtree.numOfNodes() <= maxNumOfNodes);
      assertTrue(String.format("Too short tree %d",rtree.getHeight()),
          rtree.getHeight() >= 2);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testRetrieveLeaves() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 4, 8);
      assertEquals(rtree.numOfDataEntries(), 11);
      assertEquals(2, rtree.getAllLeaves().length);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }


  public void testExpansion() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RTreeGuttman.constructFromPoints(points[0], points[1], 4, 8);
      assertEquals(0.0, rtree.Node_expansion(rtree.iRoot, 0));
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  /**
   * Read a CSV file that contains one point per line in the format "x,y".
   * The points are returned as a 2D array where the first index indicates the
   * coordinate (0 for x and 1 for y) and the second index indicates the point
   * number.
   * @param fileName
   * @return
   * @throws IOException
   */
  private double[][] readFile(String fileName) throws IOException {
    FileReader testPointsIn = new FileReader(fileName);
    char[] buffer = new char[(int) new File(fileName).length()];
    testPointsIn.read(buffer);
    testPointsIn.close();

    String[] lines = new String(buffer).split("\\s");
    double[] xs = new double[lines.length];
    double[] ys = new double[lines.length];
    for (int iLine = 0; iLine < lines.length; iLine++) {
      String[] parts = lines[iLine].split(",");
      xs[iLine] = Double.parseDouble(parts[0]);
      ys[iLine] = Double.parseDouble(parts[1]);
    }
    return new double[][]{xs, ys};
  }

}
