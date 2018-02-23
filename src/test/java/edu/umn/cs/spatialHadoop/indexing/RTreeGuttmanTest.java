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
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 11);
      assertEquals(3, rtree.numOfNodes());
      assertEquals(1, rtree.getHeight());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testBuildRectangles() {
    try {
      String fileName = "src/test/resources/test.rect";
      double[][] rects = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromRects(rects[0], rects[1], rects[2], rects[3]);
      assertEquals(rtree.numOfDataEntries(), 14);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testSearch() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      double x1 = 0, y1 = 0, x2 = 5, y2 = 4;
      assertTrue("First object should overlap the search area",
          rtree.Object_overlaps(0, x1, y1, x2, y2));
      assertTrue("Second object should overlap the search area",
          rtree.Object_overlaps(2, x1, y1, x2, y2));
      IntArray expectedResult = new IntArray();
      expectedResult.add(0);
      expectedResult.add(2);
      for (RTreeGuttman.Entry entry : rtree.search(x1, y1, x2, y2)) {
        assertTrue("Unexpected result "+entry, expectedResult.remove(entry.id));
      }
      assertTrue("Some expected results not returned "+expectedResult, expectedResult.isEmpty());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testIterateOverEntries() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      double[] x1s = points[0];
      double[] y1s = points[1];
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(11, rtree.numOfDataEntries());
      int count = 0;
      for (RTreeGuttman.Entry entry : rtree.entrySet()) {
        assertEquals(x1s[entry.id], entry.x1);
        assertEquals(y1s[entry.id], entry.y1);
        count++;
      }
      assertEquals(11, count);
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
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
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
      RTreeGuttman rtree = new RTreeGuttman(50, 100);
      rtree.initializeFromPoints(points[0], points[1]);
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
      RTreeGuttman rtree = new RTreeGuttman(2, 4);
      rtree.initializeFromPoints(points[0], points[1]);
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
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 11);
      Iterable<RTreeGuttman.Node> leaves = rtree.getAllLeaves();
      Rectangle mbrAllLeaves = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      int numOfLeaves = 0;
      for (RTreeGuttman.Node leaf : leaves) {
        mbrAllLeaves.expand(leaf.x1, leaf.y1);
        mbrAllLeaves.expand(leaf.x2, leaf.y2);
        numOfLeaves++;
      }
      assertEquals(2, numOfLeaves);
      assertEquals(new Rectangle(1, 2, 12, 12), mbrAllLeaves);
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
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
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
    int numDimensions = lines[0].split(",").length;
    double[][] coords = new double[numDimensions][lines.length];

    for (int iLine = 0; iLine < lines.length; iLine++) {
      String[] parts = lines[iLine].split(",");
      for (int iDim = 0; iDim < parts.length; iDim++)
        coords[iDim][iLine] = Double.parseDouble(parts[iDim]);
    }
    return coords;
  }

}
