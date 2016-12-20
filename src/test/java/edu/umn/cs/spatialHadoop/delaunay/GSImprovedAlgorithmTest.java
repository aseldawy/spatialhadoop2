package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for the utility class {@link Head}.
 */
public class GSImprovedAlgorithmTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public GSImprovedAlgorithmTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(GSImprovedAlgorithmTest.class);
  }

  /**
   * Test Delaunay Triangulation for a toy dataset.
   */
  public void testTriangulations() {
    String[] datasetNames = {"test_dt1", "test_dt1", "test_dt3", "test_dt4", "test_dt5"};
    try {
      for (String datasetName : datasetNames) {
        System.out.println("Testing "+datasetName);
        Point[] points = GSDTAlgorithmTest.readPoints("src/test/resources/"+datasetName+".points");
        List<Point[]> correctTriangulation = GSDTAlgorithmTest.readTriangles("src/test/resources/"+datasetName+".triangles", points);

        GSImprovedAlgorithm algo = new GSImprovedAlgorithm(points, null);
        Triangulation answer = algo.getFinalTriangulation();

        for (Point[] triangle : answer.iterateTriangles()) {
          boolean found = false;
          int i = 0;
          while (!found && i < correctTriangulation.size()) {
            found = GSDTAlgorithmTest.arrayEqualAnyOrder(triangle, correctTriangulation.get(i));
            if (found)
              correctTriangulation.remove(i);
            else
              i++;
          }
          assertTrue(String.format("Triangle (%d, %d, %d) not expected in the answer",
              Arrays.binarySearch(points, triangle[0]),
              Arrays.binarySearch(points, triangle[1]),
              Arrays.binarySearch(points, triangle[2])), found);
        }
        for (Point[] triangle : correctTriangulation) {
          System.out.printf("Triangle (%d, %d, %d) expected but not found\n",
              Arrays.binarySearch(points, triangle[0]),
              Arrays.binarySearch(points, triangle[1]),
              Arrays.binarySearch(points, triangle[2]));
        }
        assertTrue(String.format("%d triangles not found", correctTriangulation.size()),
            correctTriangulation.isEmpty());
      }
    } catch (IOException e) {
      fail("File not found");
    }
  }
}
