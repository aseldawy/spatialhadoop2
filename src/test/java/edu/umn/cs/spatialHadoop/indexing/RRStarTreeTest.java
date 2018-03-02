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