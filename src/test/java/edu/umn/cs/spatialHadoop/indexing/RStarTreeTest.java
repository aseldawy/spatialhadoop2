package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

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
      RTreeGuttman rtree = new RStarTree(4, 8);
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

  public void testBuild2() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName, 19);
      RStarTree rtree = new RStarTree(2, 8);
      rtree.initializeFromPoints(points[0], points[1]);

      Rectangle2D.Double[] expectedLeaves = {
          new Rectangle2D.Double(1, 2, 13, 1),
          new Rectangle2D.Double(3, 6, 3, 6),
          new Rectangle2D.Double(9, 6, 10, 2),
          new Rectangle2D.Double(12, 10, 3, 2),
      };
      int numFound = 0;
      for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
        System.out.println(new Rectangle(leaf.x1, leaf.y1, leaf.x2, leaf.y2).toWKT());
        for (int i = 0; i < expectedLeaves.length; i++) {
          if (expectedLeaves[i] != null && expectedLeaves[i].equals(
              new Rectangle2D.Double(leaf.x1, leaf.y1, leaf.x2-leaf.x1, leaf.y2-leaf.y1))) {
            numFound++;
          }
        }
      }
      assertEquals(expectedLeaves.length, numFound);

    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testBuild111() {
    try {
      String fileName = "src/test/resources/test111.points";
      double[][] points = readFile(fileName);
      RStarTree rtree = new RStarTree(6, 20);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 111);

      int numLeaves = 0;

      for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
        numLeaves++;
      }
      assertEquals(9, numLeaves);

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
      RTreeGuttman rtree = new RStarTree(22, 44);
      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 22);
      // Perform one split at the root
      rtree.split(rtree.root, 4);

      Iterable<RTreeGuttman.Node> leaves = rtree.getAllLeaves();
      int numOfLeaves = 0;
      for (RTreeGuttman.Node leaf : leaves)
        numOfLeaves++;
      assertEquals(2, numOfLeaves);
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
          RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, false, null);
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
          RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, false, null);

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

  public void testAuxiliarySearchStructure() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 4;
      RStarTree.AuxiliarySearchStructure aux = new RStarTree.AuxiliarySearchStructure();
      Rectangle[] partitions = RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, true, aux);
      assertEquals(3, aux.partitionGreaterThanOrEqual.length);
      assertEquals(9.0, aux.splitCoords[aux.rootSplit]);
      assertTrue(aux.partitionGreaterThanOrEqual[aux.rootSplit] < 0);
      assertTrue(aux.partitionLessThan[aux.rootSplit] >= 0);
      int p1 = aux.search(5,5);
      Rectangle expectedMBR = new Rectangle(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY,
          9, 6);
      p1 = aux.search(10,0);
      expectedMBR = new Rectangle(9, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
      assertEquals(expectedMBR, partitions[p1]);
      // Search a rectangle that matches one partition
      IntArray ps = new IntArray();
      aux.search(10, 0, 15, 15, ps);
      assertEquals(1, ps.size());
      // Make sure that it returns the same ID returned by the point search
      p1 = aux.search(10, 0);
      assertEquals(p1, ps.peek());
      // Search a rectangle that machines two partitions
      aux.search(0, 0, 5, 7, ps);
      assertEquals(2, ps.size());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testAuxiliarySearchStructureWithBiggerIndex() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 3;
      RStarTree.AuxiliarySearchStructure aux = new RStarTree.AuxiliarySearchStructure();
      Rectangle[] partitions = RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, true, aux);
      IntArray ps = new IntArray();
      // Make a search that should match with all paritions
      aux.search(0,0,100,100, ps);
      assertEquals(partitions.length, ps.size());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }


  public void testAuxiliarySearchStructureWithOnePartition() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      // Create a tree without splits
      int capacity = 100;
      RStarTree.AuxiliarySearchStructure aux = new RStarTree.AuxiliarySearchStructure();
      Rectangle[] partitions = RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, true, aux);
      int p = aux.search(0, 0);
      assertEquals(0, p);
      IntArray ps = new IntArray();
      aux.search(0, 0, 5, 5, ps);
      assertEquals(1, ps.size());
      assertEquals(0, ps.get(0));
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
      int capacity = 4;
      Rectangle[] partitions =
          RStarTree.partitionPoints(points[0], points[1], capacity/2, capacity, true, null);

      assertEquals(4, partitions.length);

      // The MBR of all partitions should cover the entire space
      Rectangle mbrAllPartitions = new Rectangle(Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (Rectangle partition : partitions)
        mbrAllPartitions.expand(partition);
      assertEquals(new Rectangle(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY,
          Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), mbrAllPartitions);

      // The partitions should not be overlapping
      for (int i = 0; i < partitions.length; i++) {
        for (int j = i + 1; j < partitions.length; j++) {
          assertFalse(String.format("Partitions %s and %s are overlapped",
              partitions[i], partitions[j]), partitions[i].isIntersected(partitions[j]));
        }
      }

    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  private double[][] readFile(String fileName) throws IOException {
    return readFile(fileName, Integer.MAX_VALUE);
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
  private double[][] readFile(String fileName, int maxLines) throws IOException {
    FileReader testPointsIn = new FileReader(fileName);
    char[] buffer = new char[(int) new File(fileName).length()];
    testPointsIn.read(buffer);
    testPointsIn.close();

    String[] lines = new String(buffer).split("\\s");
    int numLines = Math.min(maxLines, lines.length);
    double[] xs = new double[numLines];
    double[] ys = new double[numLines];
    for (int iLine = 0; iLine < numLines; iLine++) {
      String[] parts = lines[iLine].split(",");
      xs[iLine] = Double.parseDouble(parts[0]);
      ys[iLine] = Double.parseDouble(parts[1]);
    }
    return new double[][]{xs, ys};
  }

}
