package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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


  static<T> boolean arrayEqualAnyOrder(T[] a1, T[] a2) {
    if (a1.length != a2.length)
      return false;
    for (T x : a1) {
      boolean found = false;
      for (int i = 0; !found && i < a2.length; i++)
        found = x == a2[i];
      // If this element was not found, return false as the arrays are not equal
      if (!found)
        return false;
    }
    // If this point is reached, it indicates that all elements were found
    return true;
  }

  /**
   * Create a Delaunay Triangulation and partition it into safe and unsafe
   * sites and make sure that the answer is consistent, i.e., triangles
   * are not repeated.
   */
  public void testPartitioning() {
    Random random = new Random(0);
    // Generate 100 random points
    Point[] points = new Point[100];
    for (int i = 0; i < points.length; i++)
      points[i] = new Point(random.nextInt(1000), random.nextInt(1000));

    GSDTAlgorithm algo = new GSDTAlgorithm(points, null);
    SimpleGraph answer = algo.getFinalAnswerAsGraph();
    // Retrieve all triangles from the complete answer and use it as a baseline
    List<Point[]> allTriangles = new ArrayList<Point[]>();
    for (Point[] triangle : answer.iterateTriangles()) {
      allTriangles.add(triangle.clone());
    }
    // Split into a final and non-final graphs and check that we get the same
    // set of triangles from the two of them together

    SimpleGraph safe = new SimpleGraph();
    SimpleGraph unsafe = new SimpleGraph();
    algo.splitIntoFinalAndNonFinalGraphs(new Rectangle(0, 0, 1000, 1000), safe, unsafe);
    int numOfTrianglesFound = 0;
    for (Point[] safeTriangle : safe.iterateTriangles()) {
      // Check that the triangle is in the list of allTriangles
      boolean found = false;
      for (int i = 0; !found && i < allTriangles.size(); i++)
        found = arrayEqualAnyOrder(safeTriangle, allTriangles.get(i));
      assertTrue("A safe triangle not found", found);
      numOfTrianglesFound++;
    }
    // Repeat the same for unsafe triangles
    for (Point[] unsafeTriangle : unsafe.iterateTriangles()) {
      // Check that the triangle is in the list of allTriangles
      boolean found = false;
      for (int i = 0; !found && i < allTriangles.size(); i++)
        found = arrayEqualAnyOrder(unsafeTriangle, allTriangles.get(i));
      assertTrue("An unsafe triangle not found", found);
      numOfTrianglesFound++;
    }
    assertEquals(allTriangles.size(), numOfTrianglesFound);
  }
}
