package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Unit test for the RTreeGuttman class
 */
public class RStarTreeTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public RStarTreeTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(RStarTreeTest.class);
  }

  public void testBuild() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      RTreeGuttman rtree = RStarTree.constructFromPoints(points[0], points[1], 4, 8);
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
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      RStarTree rtree = RStarTree.constructFromPoints(points[0], points[1], 22, 44);
      assertEquals(rtree.numOfDataEntries(), 22);
      // Perform one split at the root
      rtree.split(rtree.iRoot, 4);
      Rectangle[] leaves = rtree.getAllLeaves();
      assertEquals(2, leaves.length);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testPartitionPoints() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 8;
      Rectangle[] partitions =
          RStarTree.partitionPoints(points[0], points[1], capacity, false);
      // Minimum number of partitions = Ceil(# points / capacity)
      int minNumPartitions = (points[0].length + capacity - 1) / capacity;
      int maxNumPartitions = (points[0].length + capacity / 2 - 1) / (capacity / 2);
      assertTrue("Too many partitions " + partitions.length,
          partitions.length <= maxNumPartitions);
      assertTrue("Too few partitions " + partitions.length,
          partitions.length >= minNumPartitions);
      // Make sure the MBR of all partitions cover the input space
      Rectangle mbrAllPartitions = partitions[0];
      for (Rectangle leaf : partitions) {
        mbrAllPartitions.expand(leaf);
      }
      assertEquals(new Rectangle(1, 2, 22, 12), mbrAllPartitions);

    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testPartition1() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 8;
      Rectangle[] partitions =
          RStarTree.partitionPoints(points[0], points[1], capacity, false);

      assertEquals(2, partitions.length);
      Arrays.sort(partitions);
      assertEquals(new Rectangle(1, 3, 6, 12), partitions[0]);
      assertEquals(new Rectangle(9, 2, 12, 10), partitions[1]);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testPartitionInfinity() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 8;
      Rectangle[] partitions =
          RStarTree.partitionPoints(points[0], points[1], capacity, true);

      assertEquals(2, partitions.length);
      Rectangle mbrAllPartitions = new Rectangle(Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);

      for (Rectangle partition : partitions) {
        mbrAllPartitions.expand(partition);
      }
      assertEquals(new Rectangle(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY,
          Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), mbrAllPartitions);
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
