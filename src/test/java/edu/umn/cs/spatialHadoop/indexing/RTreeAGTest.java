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
