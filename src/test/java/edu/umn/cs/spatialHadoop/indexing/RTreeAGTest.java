package edu.umn.cs.spatialHadoop.indexing;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Unit test for the RTreeAG class
 */
public class RTreeAGTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public RTreeAGTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(RTreeAGTest.class);
  }

  public void testBuild() {
    try {
      String fileName = "src/test/resources/test.points";
      double[][] points = readFile(fileName);
      RTreeAG rtree = new RTreeAG(points[0], points[1], 4, 8);
      assertEquals(rtree.numOfObjects(), 11);
      assertEquals(3, rtree.numOfNodes());
      assertEquals(2, rtree.getHeight());
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
      RTreeAG rtree = new RTreeAG(points[0], points[1], 4, 8);
      assertEquals(rtree.numOfObjects(), 22);
      int maxNumOfNodes = 6;
      int minNumOfNodes = 4;
      assertFalse("Too few nodes "+rtree.numOfNodes()+"<"+minNumOfNodes, rtree.numOfNodes() < minNumOfNodes);
      assertFalse("Too many nodes "+rtree.numOfNodes()+">"+maxNumOfNodes, rtree.numOfNodes() > maxNumOfNodes);
      assertEquals(2, rtree.getHeight());
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
      RTreeAG rtree = new RTreeAG(points[0], points[1], 2, 4);
      assertEquals(rtree.numOfObjects(), 22);
      int maxNumOfNodes = 18;
      int minNumOfNodes = 9;
      assertFalse("Too few nodes "+rtree.numOfNodes()+"<"+minNumOfNodes, rtree.numOfNodes() < minNumOfNodes);
      assertFalse("Too many nodes "+rtree.numOfNodes()+">"+maxNumOfNodes, rtree.numOfNodes() > maxNumOfNodes);
      assertTrue("Too short tree "+rtree.getHeight(), rtree.getHeight() >= 3);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      fail("Error working with the test file");
    }
  }
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
