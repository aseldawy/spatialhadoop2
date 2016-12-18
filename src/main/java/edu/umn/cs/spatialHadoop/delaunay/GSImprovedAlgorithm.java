package edu.umn.cs.spatialHadoop.delaunay;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;

import java.util.*;

/**
 * A variation of the Guibas and Stolif's algorithm for Delaunay Triangulation.
 * Instead of recursively partitioning records based on one dimension (e.g., X,)
 * points are partitioned based on either X or Y coordinate based on which
 * axis is longer. This techniques minimizes the chance of having a partition
 * with three co-linear or almost co-linear points.
 * @author Ahmed Eldawy
 *
 */
public class GSImprovedAlgorithm extends GSDTAlgorithm {

  static final Log LOG = LogFactory.getLog(GSImprovedAlgorithm.class);
  /**Maximum number of points to partition recursively*/
  final int MaxNumPointsToPartition = 10;

  public <P extends Point> GSImprovedAlgorithm(P[] inPoints, Progressable progress) {
    super(inPoints, progress);
  }

  public GSImprovedAlgorithm(Triangulation[] ts, Progressable progress) {
    super(ts, progress);
  }

  @Override
  protected IntermediateTriangulation computeTriangulation(int start, int end) {
    // Sort all points and record the order of merging
    class Part extends Rectangle {
      int start;
      int end;
      // True if sorted by x; false if sorted by y
      boolean sorted_x;

      Part(int start, int end) {
        this.start = start;
        this.end = end;
      }
    };

    QuickSort quickSort = new QuickSort();
    IndexedSortable xSorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        if (points[i].x < points[j].x)
          return -1;
        if (points[i].x > points[j].x)
          return 1;
        if (points[i].y < points[j].y)
          return -1;
        if (points[i].y > points[j].y)
          return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        Point t = points[i];
        points[i] = points[j];
        points[j] = t;
      }
    };
    IndexedSortable ySorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        if (points[i].y < points[j].y)
          return -1;
        if (points[i].y > points[j].y)
          return 1;
        if (points[i].x < points[j].x)
          return -1;
        if (points[i].x > points[j].x)
          return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        Point t = points[i];
        points[i] = points[j];
        points[j] = t;
      }
    };

    // Partitions to be sorted, a null entry indicates that it is time to merge
    // the top two entries in the triangulations to be merged
    Stack<Part> toPartition = new Stack<Part>();
    // Triangulations to merge
    Stack<IntermediateTriangulation> toMerge = new Stack<IntermediateTriangulation>();

    // Compute the MBR of the input
    Part topPart = new Part(start, end);
    topPart.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Point p : points)
      topPart.expand(p);

    if (topPart.getWidth() > topPart.getHeight()) {
      // Partition along the X-axis
      quickSort.sort(xSorter, start, end);
      topPart.sorted_x = true;
      toPartition.push(topPart);
    } else {
      // Partition along the Y-axis
      quickSort.sort(ySorter, start, end);
      topPart.sorted_x = false;
      toPartition.push(topPart);
    }

    while (!toPartition.isEmpty()) {
      if (progress != null)
        progress.progress();
      Part currentPart = toPartition.pop();
      if (currentPart == null) {
        // Merge the top two triangulations
        IntermediateTriangulation partial1 = toMerge.pop();
        IntermediateTriangulation partial2 = toMerge.pop();
        IntermediateTriangulation merged = merge(partial1, partial2);
        toMerge.push(merged);
      } else if (currentPart.end - currentPart.start < MaxNumPointsToPartition){
        // Compute the points directly using the regular Guibas and Stolif's algorithm
        IntermediateTriangulation partialAnswer = super.computeTriangulation(currentPart.start, currentPart.end);
        toMerge.push(partialAnswer);
      } else {
        // Further partition into two along the longer dimension
        if (currentPart.getWidth() > currentPart.getHeight()) {
          if (!currentPart.sorted_x)
            // Mot sorted correctly, sort that part
            quickSort.sort(xSorter, currentPart.start, currentPart.end);
          int middle = (currentPart.start + currentPart.end) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // Create left partition
          Part left = new Part(currentPart.start, middle);
          left.set(currentPart.x1, currentPart.y1, points[middle].x, currentPart.y2);
          left.sorted_x = true;
          toPartition.push(left);
          // Create right partition
          Part right = new Part(middle, currentPart.end);
          right.set(points[middle].x, currentPart.y1, currentPart.x2, currentPart.y2);
          right.sorted_x = true;
          toPartition.push(right);
        } else {
          if (currentPart.sorted_x)
            // Not sorted correctly, sort that part
            quickSort.sort(ySorter, currentPart.start, currentPart.end);
          // Partition along the Y-axis
          int middle = (currentPart.start + currentPart.end) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // Create upper partition
          Part upper = new Part(currentPart.start, middle);
          upper.set(currentPart.x1, currentPart.y1, currentPart.x2, points[middle].y);
          upper.sorted_x = false;
          toPartition.push(upper);
          // Create lower partition
          Part lower = new Part(middle, currentPart.end);
          lower.set(currentPart.x1, points[middle].y, currentPart.x2, currentPart.y2);
          lower.sorted_x = false;
          toPartition.push(lower);
        }
      }
    }

    if (toMerge.size() != 1)
      throw new RuntimeException("Expected exactly one file answer but found " + toMerge.size());

    return toMerge.pop();
  }
}
