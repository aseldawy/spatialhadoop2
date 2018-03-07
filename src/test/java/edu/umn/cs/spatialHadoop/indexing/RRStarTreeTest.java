package edu.umn.cs.spatialHadoop.indexing;

import junit.framework.TestCase;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class RRStarTreeTest extends TestCase {

  public void testDOvlp() {
    Rectangle2D.Double[] entries = {
        new Rectangle2D.Double(0, 0, 3, 2),
        new Rectangle2D.Double(2, 1, 2, 3),
        new Rectangle2D.Double(3, 3, 1, 1),
    };
    RRStarTree rrStarTree = createRRStarWithDataEntries(entries);
    int t = rrStarTree.Node_createNodeWithChildren(true, 0);
    int j = rrStarTree.Node_createNodeWithChildren(true, 1);
    assertEquals(5.0, rrStarTree.Node_perimeter(t));
    assertEquals(5.0, rrStarTree.Node_perimeter(j));
    int o = 2;
    double dOvlpPerim = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.PERIMETER);
    assertEquals(3.0, dOvlpPerim);
    double dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(5.0, dOvlpVol);

    // Test when t and j are disjoint
    entries[0] = new Rectangle2D.Double(0, 0, 2, 1);
    rrStarTree = createRRStarWithDataEntries(entries);
    t = rrStarTree.Node_createNodeWithChildren(true, 0);
    j = rrStarTree.Node_createNodeWithChildren(true, 2);
    o = 1;
    dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(1.0, dOvlpVol);

    // Test when (t U o) and j are disjoint
    entries[2] = new Rectangle2D.Double(5, 5, 1, 1);
    rrStarTree = createRRStarWithDataEntries(entries);
    t = rrStarTree.Node_createNodeWithChildren(true, 0);
    j = rrStarTree.Node_createNodeWithChildren(true, 2);
    o = 1;
    dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(0.0, dOvlpVol);
  }

  public void testCovers() {
    Rectangle2D.Double[] entries = {
        new Rectangle2D.Double(0, 0, 3, 2),
        new Rectangle2D.Double(2, 1, 2, 3),
        new Rectangle2D.Double(3, 3, 1, 1),
    };
    RRStarTree rrStarTree = createRRStarWithDataEntries(entries);
    int n1 = rrStarTree.Node_createNodeWithChildren(true, 0);
    int n2 = rrStarTree.Node_createNodeWithChildren(true, 1);
    assertFalse(rrStarTree.Node_covers(n1, 2));
    assertTrue(rrStarTree.Node_covers(n2, 2));
  }

  public void testInsertion() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      double[] xs = points[0], ys = points[1];
      // Case 1: COV is not empty, choose the node with zero volume
      RRStarTree rtree = new RRStarTree(4, 8);
      rtree.initializeDataEntries(xs, ys);
      int n1 = rtree.Node_createNodeWithChildren(true, 1, 7);
      int n2 = rtree.Node_createNodeWithChildren(true, 5, 6);
      rtree.iRoot = rtree.Node_createNodeWithChildren(false, n1, n2);
      // n1 and n2 cover p4 but n1 has a zero volume, p4 should be added to n1
      rtree.insertAnExistingDataEntry(4);
      assertEquals(3, rtree.Node_size(n1));
      assertEquals(2, rtree.Node_size(n2));

      // Case 2: COV is not empty, multiple nodes with zero volume, choose the one with min perimeter
      rtree = new RRStarTree(4, 8);
      xs[6] = 5; ys[6] = 7;
      rtree.initializeDataEntries(xs, ys);
      n1 = rtree.Node_createNodeWithChildren(true, 1, 7);
      n2 = rtree.Node_createNodeWithChildren(true, 5, 6);
      rtree.iRoot = rtree.Node_createNodeWithChildren(false, n1, n2);
      // Both n1 and n2 cover p4 with zero volume, but n2 has a smaller perimeter
      rtree.insertAnExistingDataEntry(4);
      assertEquals(2, rtree.Node_size(n1));
      assertEquals(3, rtree.Node_size(n2));

      // Case 3: COV is not empty, all have non-zero volume, choose the one with min volume
      rtree = new RRStarTree(4, 8);
      points = readFile(fileName); // Reload the file
      xs = points[0]; ys = points[1];
      rtree.initializeDataEntries(xs, ys);
      n1 = rtree.Node_createNodeWithChildren(true, 3, 9);
      n2 = rtree.Node_createNodeWithChildren(true, 2, 10);
      rtree.iRoot = rtree.Node_createNodeWithChildren(false, n1, n2);
      // Both n1 and n2 cover p4 but n1 has a smaller volume
      rtree.insertAnExistingDataEntry(4);
      assertEquals(3, rtree.Node_size(n1));
      assertEquals(2, rtree.Node_size(n2));

      // Case 4: Some node has to be expanded. We add it to the node with smallest
      // deltaPerim if this would not increase the perimetric overlap
      rtree = new RRStarTree(4, 8);
      points = readFile(fileName); // Reload the file
      xs = points[0]; ys = points[1];
      rtree.initializeDataEntries(xs, ys);
      n1 = rtree.Node_createNodeWithChildren(true, 4, 6);
      n2 = rtree.Node_createNodeWithChildren(true, 7, 10);
      int n3 = rtree.Node_createNodeWithChildren(true, 2, 8);
      rtree.iRoot = rtree.Node_createNodeWithChildren(false, n1, n2, n3);
      rtree.insertAnExistingDataEntry(1);
      assertEquals(3, rtree.Node_size(n1));
      assertEquals(2, rtree.Node_size(n2));
      assertEquals(2, rtree.Node_size(n3));

      // Case 5: Recreate the same example in Figure 1 on page 802 in the paper
      double[] x1s = new double[7];
      double[] y1s = new double[7];
      double[] x2s = new double[7];
      double[] y2s = new double[7];
      x1s[1] = 1; y1s[1] = 1; x2s[1] = 6; y2s[1] = 6; // E1
      x1s[2] = 0; y1s[2] = 0; x2s[2] = 4; y2s[2] = 5; // E2
      x1s[3] = 2; y1s[3] = 5.9; x2s[3] = 4; y2s[3] = 8; // E3
      x1s[4] = 6.2; y1s[4] = 1; x2s[4] = 8; y2s[4] = 3; // E4
      x1s[5] = 9; y1s[5] = 8; x2s[5] = 11; y2s[5] = 10; // E5
      x1s[6] = 5; y1s[6] = 5; x2s[6] = 7; y2s[6] = 7; // Omega
      rtree = new RRStarTree(4, 8);
      rtree.initializeDataEntries(x1s, y1s, x2s, y2s);
      int e1 = rtree.Node_createNodeWithChildren(true, 1);
      int e2 = rtree.Node_createNodeWithChildren(true, 2);
      int e3 = rtree.Node_createNodeWithChildren(true, 3);
      int e4 = rtree.Node_createNodeWithChildren(true, 4);
      int e5 = rtree.Node_createNodeWithChildren(true, 5);
      rtree.iRoot = rtree.Node_createNodeWithChildren(false, e1, e2, e3, e4, e5);

      rtree.insertAnExistingDataEntry(6); // Insert omega
      // It should be added to E3 as described in the paper
      assertEquals(2, rtree.Node_size(e3));
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testBuild() {
    try {
      String fileName = "src/test/resources/test2.points";
      double[][] points = readFile(fileName);
      RRStarTree rtree = new RRStarTree(4, 8);
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
      double[][] points = readFile(fileName);
      RRStarTree rtree = new RRStarTree(6, 12);
      rtree.initializeFromPoints(points[0], points[1]);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }

  public void testSplit() {
    try {
      String fileName = "src/test/resources/test3.points";
      double[][] points = readFile(fileName);
      RRStarTree rtree = new RRStarTree(2, 9);

      rtree.initializeFromPoints(points[0], points[1]);
      assertEquals(rtree.numOfDataEntries(), 10);
      Rectangle2D.Double[] expectedLeaves = {
        new Rectangle2D.Double(1, 2, 5, 10),
        new Rectangle2D.Double(7,3,6,4)
      };
      int numFound = 0;
      for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
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

  private RRStarTree createRRStarWithDataEntries(Rectangle2D.Double[] entries) {
    double[] x1s = new double[entries.length];
    double[] y1s = new double[entries.length];
    double[] x2s = new double[entries.length];
    double[] y2s = new double[entries.length];
    for (int i = 0; i < entries.length; i++) {
      x1s[i] = entries[i].getMinX();
      y1s[i] = entries[i].getMinY();
      x2s[i] = entries[i].getMaxX();
      y2s[i] = entries[i].getMaxY();
    }
    RRStarTree rrStarTree = new RRStarTree(4, 8);
    rrStarTree.initializeDataEntries(x1s, y1s, x2s, y2s);
    return rrStarTree;
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