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
   * Test Delaunay Triangulation for a toy dataset. Visualized in file test_dt1.svg
   */
  /*public void testTriangulation1() {
    Point[] points = {
        new Point(50, 50),
        new Point(60, 10),
        new Point(70, 80),
        new Point(80, 50),
    };
    Point[][] correctTriangulation = {
        new Point[] {points[0], points[1], points[3]},
        new Point[] {points[0], points[3], points[2]},
    };

    GSDTAlgorithm algo = new GSDTAlgorithm(points, null);
    SimpleGraph answer = algo.getFinalAnswerAsGraph();

    int numOfTrianglesFound = 0;
    for (Point[] unsafeTriangle : answer.iterateTriangles()) {
      boolean found = false;
      for (int i = 0; !found && i < correctTriangulation.length; i++)
        found = arrayEqualAnyOrder(unsafeTriangle, correctTriangulation[i]);
      assertTrue("A triangle not found", found);
      numOfTrianglesFound++;
    }
    assertEquals(correctTriangulation.length, numOfTrianglesFound);
  }*/

  /**
   * Test Delaunay Triangulation with a small dataset
   */
  public void testTriangulation2() {
    Point[] points = {
        new Point(27, 75),
        new Point(75, 25),
        new Point(65, 50),
        new Point(18, 69),
        new Point(11, 89),
        new Point(49, 95),
        new Point(95, 54),
        new Point(34, 13),
        new Point(58, 14),
        new Point(22, 25),
    };
    Point[][] correctTriangulation = {
        new Point[] {points[2], points[7], points[8]},
        new Point[] {points[9], points[3], points[4]},
        new Point[] {points[3], points[0], points[4]},
        new Point[] {points[0], points[5], points[4]},
        new Point[] {points[2], points[9], points[7]},
        new Point[] {points[0], points[2], points[5]},
        new Point[] {points[5], points[2], points[6]},
        new Point[] {points[1], points[6], points[2]},
        new Point[] {points[1], points[2], points[8]},
        new Point[] {points[0], points[3], points[2]},
        new Point[] {points[2], points[3], points[9]},
    };

    GSDTAlgorithm algo = new GSDTAlgorithm(points, null);
    SimpleGraph answer = algo.getFinalAnswerAsGraph();

    int numOfTrianglesFound = 0;
    for (Point[] triangle : answer.iterateTriangles()) {
      boolean found = false;
      for (int i = 0; !found && i < correctTriangulation.length; i++)
        found = arrayEqualAnyOrder(triangle, correctTriangulation[i]);
      assertTrue(String.format("Triangle #%d (%f, %f), (%f, %f), (%f, %f) not found",
          numOfTrianglesFound,
          triangle[0].x, triangle[0].y,
          triangle[1].x, triangle[1].y,
          triangle[2].x, triangle[2].y), found);
      numOfTrianglesFound++;
    }
    assertEquals(correctTriangulation.length, numOfTrianglesFound);
  }

  /**
   * Create a Delaunay Triangulation and partition it into safe and unsafe
   * sites and make sure that the answer is consistent, i.e., triangles
   * are not repeated.
   *//*
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
  }*/
}
