package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.MemoryInputStream;
import edu.umn.cs.spatialHadoop.util.IntArray;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.nio.ByteBuffer;

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
      int iNode = rtree.root;
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
      assertEquals(0.0, rtree.Node_volumeExpansion(rtree.root, 0));
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testWrite() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 11);
      assertEquals(3, rtree.numOfNodes());
      assertEquals(1, rtree.getHeight());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      rtree.write(dos, null);
      dos.close();

      ByteBuffer serializedTree = ByteBuffer.wrap(baos.toByteArray());
      int footerOffset = serializedTree.getInt(serializedTree.limit() - 8);
      // 0 bytes per data entry + 4 bytes per node (# of children) + (4 (offset) + 32 (MBR)) for each child (root excluded)
      final int expectedFooterOffset = 11 * 0 + 3 * 4 + (14 - 1) * (8 * 4 + 4);
      assertEquals(expectedFooterOffset, footerOffset);
      // Check the MBR of the root
      double x1 = serializedTree.getDouble(footerOffset);
      double y1 = serializedTree.getDouble(footerOffset + 8);
      double x2 = serializedTree.getDouble(footerOffset + 16);
      double y2 = serializedTree.getDouble(footerOffset + 24);
      assertEquals(1.0, x1);
      assertEquals(2.0, y1);
      assertEquals(12.0, x2);
      assertEquals(12.0, y2);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testRead() {
    byte[] treeBytes = null;
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 11);
      assertEquals(3, rtree.numOfNodes());
      assertEquals(1, rtree.getHeight());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      rtree.write(dos, null);
      dos.close();
      treeBytes = baos.toByteArray();
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }

    try {
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      FSDataInputStream fsdis = new FSDataInputStream(new MemoryInputStream(treeBytes));
      rtree.readFields(fsdis, treeBytes.length, null);
      IntArray results = new IntArray();
      rtree.search(0, 0, 4.5, 10, results);
      assertEquals(3, results.size());
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error opening the tree");
    }
  }

  public void testReadMultilevelFile() {
    byte[] treeBytes = null;
    try {
      String fileName = "src/test/resources/test111.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 111);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      rtree.write(dos, null);
      dos.close();
      treeBytes = baos.toByteArray();
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }

    try {
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      FSDataInputStream fsdis = new FSDataInputStream(new MemoryInputStream(treeBytes));
      rtree.readFields(fsdis, treeBytes.length, null);
      int resultSize = 0;
      for (Object o : rtree.entrySet()) {
        resultSize++;
      }
      assertEquals(111, resultSize);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error opening the tree");
    }
  }

  public void testHollowRTree() {
    try {
      String fileName = "src/test/resources/test.rect";
      double[][] rects = readFile(fileName);
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeHollowRTree(rects[0], rects[1], rects[2], rects[3]);
      int numLeaves = 0;
      for (Object leaf : rtree.getAllLeaves())
        numLeaves++;
      assertEquals(14, numLeaves);
      assertEquals(0, rtree.numOfDataEntries());

      int i = rtree.noInsert(999, 200, 999, 200);
      assertEquals(12, i);
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
