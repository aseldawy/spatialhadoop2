package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for the utility class {@link Head}.
 */
public class GSDTAlgorithmTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public GSDTAlgorithmTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(GSDTAlgorithmTest.class);
  }


  // Test example is copied from https://www.mathworks.com/help/matlab/examples/creating-and-editing-delaunay-triangulations.html
  double[] testPoints = {
      0.2760,    0.7513,
      0.6797,    0.2551,
      0.6551,    0.5060,
      0.1626,    0.6991,
      0.1190,    0.8909,
      0.4984,    0.9593,
      0.9597,    0.5472,
      0.3404,    0.1386,
      0.5853,    0.1493,
      0.2238,    0.2575,
  };

  int[] triangles = {
      4,    10,     1,
      3,     8,     9,
      10,    4,     5,
      4,     1,     5,
      1,     6,     5,
      3,    10,     8,
      1,     3,     6,
      2,     9,     7,
      6,     3,     7,
      1,    10,     3,
  };

  public void testPlainTextFile() {
    // Test a simple scenario
    List<Point> points = new ArrayList<Point>(testPoints.length/2);
    for (int i = 0; i < testPoints.length; i+=2) {
      points.add(new Point(testPoints[i], testPoints[i+1]));
    }

    GSDTAlgorithm algo = new GSDTAlgorithm(points.toArray(new Point[points.size()]), null);
    SimpleGraph answer = algo.getFinalAnswerAsGraph();
    answer.draw();
    // Check that the edges are correct
    for (int iEdge = 0; iEdge < answer.edgeStarts.length; iEdge++) {
      // Check all the three edges of the triangle
      // Notice that the order of points in the answer could be different than the input
      int edgeStart = points.indexOf(answer.sites[answer.edgeStarts[iEdge]]) + 1;
      int edgeEnd = points.indexOf(answer.sites[answer.edgeEnds[iEdge]]) + 1;

      boolean found = false;
      for (int iTriangle = 0; !found && iTriangle < triangles.length; iTriangle+=3) {
        // An edge is found if both edge start and ends are in the same triangle
        boolean startFound = edgeStart == triangles[iTriangle] || edgeStart == triangles[iTriangle+1] ||
            edgeStart == triangles[iTriangle+2];
        boolean endFound = edgeEnd == triangles[iTriangle] || edgeEnd == triangles[iTriangle+1] ||
            edgeEnd == triangles[iTriangle+2];
        found = startFound && endFound;
      }
      assertTrue(String.format("Edge (%d, %d) not found", edgeStart, edgeEnd), found);
    }
  }
}
