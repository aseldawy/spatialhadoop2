package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
   * Read all points from the given file
   * @param filename
   * @return
   */
  static Point[] readPoints(String filename) throws IOException {
    List<Point> points = new ArrayList<Point>();
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split(",");
      points.add(new Point(Double.parseDouble(parts[0]), Double.parseDouble(parts[1])));
    }
    reader.close();

    return points.toArray(new Point[points.size()]);
  }

  /**
   * Read a list of triangles from a file.
   * @param filename
   * @param points A list of all points. Used to avoid creating duplicate objects
   *               of each point.
   * @return
   */
  static List<Point[]> readTriangles(String filename, Point[] points) throws IOException {
    List<Point[]> triangles = new ArrayList<Point[]>();
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split("[,\t]");
      double[] coords = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        coords[i] = Double.parseDouble(parts[i]);
      Point[] triangle = new Point[3];
      for (Point pt : points) {
        if (pt.x == coords[0] && pt.y == coords[1])
          triangle[0] = pt;
        else if (pt.x == coords[2] && pt.y == coords[3])
          triangle[1] = pt;
        else if (pt.x == coords[4] && pt.y == coords[5])
          triangle[2] = pt;
      }
      triangles.add(triangle);
    }
    reader.close();
    return triangles;
  }


  /**
   * Test Delaunay Triangulation for a toy dataset. Visualized in file test_dt1.svg
   */
  public void testTriangulations() {
    String[] datasetNames = {"test_dt1", "test_dt2", "test_dt3"};
    try {
      for (String datasetName : datasetNames) {
        Point[] points = readPoints("src/test/resources/"+datasetName+".points");
        List<Point[]> correctTriangulation = readTriangles("src/test/resources/"+datasetName+".triangles", points);

        GSDTAlgorithm algo = new GSDTAlgorithm(points, null);
        Triangulation answer = algo.getFinalTriangulation();

        int iTriangle = 0;
        for (Point[] triangle : answer.iterateTriangles()) {
          boolean found = false;
          int i = 0;
          while (!found && i < correctTriangulation.size()) {
            found = arrayEqualAnyOrder(triangle, correctTriangulation.get(i));
            if (found)
              correctTriangulation.remove(i);
            else
              i++;
          }
          assertTrue(String.format("Triangle #%d (%f, %f), (%f, %f), (%f, %f) not found",
              iTriangle,
              triangle[0].x, triangle[0].y,
              triangle[1].x, triangle[1].y,
              triangle[2].x, triangle[2].y), found);
          iTriangle++;
        }
        for (Point[] triangle : correctTriangulation) {
          System.out.printf("Triangle not found (%f, %f) (%f, %f) (%f, %f)\n",
              triangle[0].x, triangle[0].y,
              triangle[1].x, triangle[1].y,
              triangle[2].x, triangle[2].y
          );
        }
        assertTrue(String.format("%d triangles not found", correctTriangulation.size()),
            correctTriangulation.isEmpty());
      }
    } catch (IOException e) {
      fail("File not found");
    }
  }

  /**
   * Test that partitioning a Delaunay triangulation into final and non-final
   * ones does not change the reported triangles.
   */
  public void testPartitioning() {
    try {
      Point[] points = readPoints("src/test/resources/test_dt3.points");
      List<Point[]> allTriangles = readTriangles("src/test/resources/test_dt3.triangles", points);

      // Split into a final and non-final graphs and check that we get the same
      // set of triangles from the two of them together
      Triangulation safe = new Triangulation();
      Triangulation unsafe = new Triangulation();
      GSDTAlgorithm algo = new GSDTAlgorithm(points, null);
      algo.splitIntoSafeAndUnsafeGraphs(new Rectangle(0, 0, 1000, 1000), safe, unsafe);
      for (Point[] safeTriangle : safe.iterateTriangles()) {
        // Check that the triangle is in the list of allTriangles
        boolean found = false;
        int i = 0;
        while (!found && i < allTriangles.size()) {
          found = arrayEqualAnyOrder(safeTriangle, allTriangles.get(i));
          if (found)
            allTriangles.remove(i);
          else
            i++;
        }

        assertTrue(String.format("Safe triangle (%d, %d, %d) not found",
            Arrays.binarySearch(points, safeTriangle[0]),
            Arrays.binarySearch(points, safeTriangle[1]),
            Arrays.binarySearch(points, safeTriangle[2])), found);
      }
      // For unsafe triangles, invert the reportedSites to report all sites that
      // have never been reported before.
      unsafe.sitesToReport = unsafe.reportedSites.invert();
      for (Point[] unsafeTriangle : unsafe.iterateTriangles()) {
        // Check that the triangle is in the list of allTriangles
        boolean found = false;
        int i = 0;
        while (!found && i < allTriangles.size()) {
          found = arrayEqualAnyOrder(unsafeTriangle, allTriangles.get(i));
          if (found)
            allTriangles.remove(i);
          else
            i++;
        }

        assertTrue(String.format("Unsafe triangle (%d, %d, %d) not found",
            Arrays.binarySearch(points, unsafeTriangle[0]),
            Arrays.binarySearch(points, unsafeTriangle[1]),
            Arrays.binarySearch(points, unsafeTriangle[2])), found);
      }
      for (Point[] triangle : allTriangles) {
        System.out.printf("Triangle not found (%d, %d, %d)\n",
            Arrays.binarySearch(points, triangle[0]),
            Arrays.binarySearch(points, triangle[1]),
            Arrays.binarySearch(points, triangle[2])
        );
      }
      assertTrue(String.format("%d triangles not found", allTriangles.size()),
          allTriangles.isEmpty());
    } catch (IOException e) {
      fail("File not found");
    }
  }

  /**
   * Tests the the calculation of DT in a distributed manner reports the correct
   * triangles.
   */
  public void testMerge() {
    try {
      Point[] points = readPoints("src/test/resources/test_dt3.points");
      List<Point[]> allTriangles = readTriangles("src/test/resources/test_dt3.triangles", points);
      // Split the input set of points vertically, compute each DT separately,
      // then merge them and make sure that we get the same answer
      // Points are already sorted on the x-axis
      Point[] leftHalf = new Point[points.length / 2];
      System.arraycopy(points, 0, leftHalf, 0, leftHalf.length);

      GSDTAlgorithm leftAlgo = new GSDTAlgorithm(leftHalf, null);
      Rectangle leftMBR = new Rectangle(0, 0, (points[leftHalf.length - 1].x + points[leftHalf.length].x) / 2, 1000);
      Triangulation leftSafe = new Triangulation();
      Triangulation leftUnsafe = new Triangulation();
      leftAlgo.splitIntoSafeAndUnsafeGraphs(leftMBR, leftSafe, leftUnsafe);

      // Check all final triangles on the left half
      for (Point[] safeTriangle : leftSafe.iterateTriangles()) {
        // Check that the triangle is in the list of allTriangles
        boolean found = false;
        int i = 0;
        while (!found && i < allTriangles.size()) {
          found = arrayEqualAnyOrder(safeTriangle, allTriangles.get(i));
          if (found)
            allTriangles.remove(i);
          else
            i++;
        }

        assertTrue(String.format("Safe triangle (%d, %d, %d) not found",
            Arrays.binarySearch(points, safeTriangle[0]),
            Arrays.binarySearch(points, safeTriangle[1]),
            Arrays.binarySearch(points, safeTriangle[2])), found);
      }

      // Repeat the same thing for the right half.
      Point[] rightHalf = new Point[points.length - leftHalf.length];
      System.arraycopy(points, leftHalf.length, rightHalf, 0, rightHalf.length);

      // Compute DT for the right half
      GSDTAlgorithm rightAlgo = new GSDTAlgorithm(rightHalf, null);
      Rectangle rightMBR = new Rectangle((points[leftHalf.length - 1].x + points[leftHalf.length].x) / 2, 0, 1000, 1000);
      Triangulation rightSafe = new Triangulation();
      Triangulation rightUnsafe = new Triangulation();
      rightAlgo.splitIntoSafeAndUnsafeGraphs(rightMBR, rightSafe, rightUnsafe);

      // Check all final triangles on the right half
      for (Point[] safeTriangle : rightSafe.iterateTriangles()) {
        // Check that the triangle is in the list of allTriangles
        boolean found = false;
        int i = 0;
        while (!found && i < allTriangles.size()) {
          found = arrayEqualAnyOrder(safeTriangle, allTriangles.get(i));
          if (found)
            allTriangles.remove(i);
          else
            i++;
        }

        assertTrue(String.format("Safe triangle (%d, %d, %d) not found",
            Arrays.binarySearch(points, safeTriangle[0]),
            Arrays.binarySearch(points, safeTriangle[1]),
            Arrays.binarySearch(points, safeTriangle[2])), found);
      }

      // Merge the unsafe parts from the left and right to finalize
      GSDTAlgorithm mergeAlgo = new GSDTAlgorithm(new Triangulation[] {leftUnsafe, rightUnsafe}, null);
      Triangulation finalPart = mergeAlgo.getFinalTriangulation();

      for (Point[] safeTriangle : finalPart.iterateTriangles()) {
        // Check that the triangle is in the list of allTriangles
        boolean found = false;
        int i = 0;
        while (!found && i < allTriangles.size()) {
          found = arrayEqualAnyOrder(safeTriangle, allTriangles.get(i));
          if (found)
            allTriangles.remove(i);
          else
            i++;
        }

        assertTrue(String.format("Safe triangle (%d, %d, %d) not found",
            Arrays.binarySearch(points, safeTriangle[0]),
            Arrays.binarySearch(points, safeTriangle[1]),
            Arrays.binarySearch(points, safeTriangle[2])), found);
      }

      for (Point[] triangle : allTriangles) {
        System.out.printf("Triangle not found (%d, %d, %d)\n",
            Arrays.binarySearch(points, triangle[0]),
            Arrays.binarySearch(points, triangle[1]),
            Arrays.binarySearch(points, triangle[2])
        );
      }
      assertTrue(String.format("%d triangles not found", allTriangles.size()),
          allTriangles.isEmpty());
    } catch (IOException e) {
      fail("File not found");
    }

  }
}
