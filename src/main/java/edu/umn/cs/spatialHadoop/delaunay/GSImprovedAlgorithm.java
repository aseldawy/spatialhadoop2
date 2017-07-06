package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Progressable;

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

  public <P extends Point> GSImprovedAlgorithm(P[] inPoints, Progressable progress) {
    super(inPoints, progress);
  }

  public GSImprovedAlgorithm(Triangulation[] ts, Progressable progress) {
    super(ts, progress);
  }

  @Override
  protected IntermediateTriangulation computeTriangulation(int rstart, int rend) {
    final Point[] sortedX = this.points;
    final Point[] sortedY = this.points.clone();

    // Sort all points and record the order of merging
    class Part {
      int pStart;
      int pEnd;

      Part(int start, int end) {
        this.pStart = start;
        this.pEnd = end;
      }

      @Override
      public String toString() {
        return "Partition: "+ pStart +","+ pEnd;
      }
    };

    // Sort the list by X
    Comparator<Point> comparatorX = new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        int dx = Double.compare(p1.x, p2.x);
        if (dx != 0)
          return dx;
        return Double.compare(p1.y, p2.y);
      }
    };
    Arrays.sort(sortedX, comparatorX);
    // Sort the list by Y
    Comparator<Point> comparatorY = new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        int dy = Double.compare(p1.y, p2.y);
        if (dy != 0)
          return dy;
        return Double.compare(p1.x, p2.x);
      }
    };
    Arrays.sort(sortedY, comparatorY);

    // Partitions to be sorted, a null entry indicates that it is time to merge
    // the top two entries in the triangulations to be merged
    Stack<Part> toPartition = new Stack<Part>();
    // Triangulations to merge
    Stack<IntermediateTriangulation> toMerge = new Stack<IntermediateTriangulation>();

    // Compute the MBR of the input
    toPartition.push(new Part(rstart, rend));
    long reportedTime = 0;
    // A temporary array to partition sorted arrays
    Point[] newSortedRange = new Point[points.length];

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
      } else if (currentPart.pEnd - currentPart.pStart == 4) {
        xs[currentPart.pStart] = points[currentPart.pStart].x;
        ys[currentPart.pStart] = points[currentPart.pStart].y;
        xs[currentPart.pStart +1] = points[currentPart.pStart +1].x;
        ys[currentPart.pStart +1] = points[currentPart.pStart +1].y;
        xs[currentPart.pStart +2] = points[currentPart.pStart +2].x;
        ys[currentPart.pStart +2] = points[currentPart.pStart +2].y;
        xs[currentPart.pStart +3] = points[currentPart.pStart +3].x;
        ys[currentPart.pStart +3] = points[currentPart.pStart +3].y;
        // Compute DT for every two points
        toPartition.push(null);
        toMerge.push(new IntermediateTriangulation(currentPart.pStart + 2, currentPart.pStart + 3));
        toMerge.push(new IntermediateTriangulation(currentPart.pStart, currentPart.pStart + 1));
      } else if (currentPart.pEnd - currentPart.pStart == 3) {
        xs[currentPart.pStart] = points[currentPart.pStart].x;
        ys[currentPart.pStart] = points[currentPart.pStart].y;
        xs[currentPart.pStart +1] = points[currentPart.pStart +1].x;
        ys[currentPart.pStart +1] = points[currentPart.pStart +1].y;
        xs[currentPart.pStart +2] = points[currentPart.pStart +2].x;
        ys[currentPart.pStart +2] = points[currentPart.pStart +2].y;
        // Compute for three points
        toMerge.push(new IntermediateTriangulation(currentPart.pStart, currentPart.pStart + 1, currentPart.pStart + 2));
      } else if (currentPart.pEnd - currentPart.pStart == 2) {
        xs[currentPart.pStart] = points[currentPart.pStart].x;
        ys[currentPart.pStart] = points[currentPart.pStart].y;
        xs[currentPart.pStart +1] = points[currentPart.pStart +1].x;
        ys[currentPart.pStart +1] = points[currentPart.pStart +1].y;
        // Two points, connect with a line
        toMerge.push(new IntermediateTriangulation(currentPart.pStart, currentPart.pStart + 1));
      } else {
        // Further partition into two along the longer dimension
        double width = sortedX[currentPart.pEnd -1].x - sortedX[currentPart.pStart].x;
        double height = sortedY[currentPart.pEnd -1].y - sortedY[currentPart.pStart].y;
        if (width == 0 || height == 0) {
          // All points form one line, use all of them together as an intermediate
          // triangulation
          for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
            xs[i] = points[i].x;
            ys[i] = points[i].y;
          }
          // Notice that the range of currentPart is exclusive of the end point
          // while the range in IntermediateTriangulation is inclusive
          toMerge.push(new IntermediateTriangulation(currentPart.pStart, currentPart.pEnd-1));
        } else {
          int middle = (currentPart.pStart + currentPart.pEnd) / 2;
          int position1 = currentPart.pStart;
          int position2 = middle;
          Point[] arrayToPartition;
          Comparator<Point> comparator;
          Point middlePoint;
          if (width > height) {
            // Split the sortedY list into two lists, left and right, around
            // the middle point
            middlePoint = sortedX[middle];
            comparator = comparatorX;
            arrayToPartition = sortedY;
          } else {
            // Partition along the Y-axis
            // Split the sortedX list around the middle point into top and bottom
            // lists, each of them is separately sorted by X.
            comparator = comparatorY;
            middlePoint = sortedY[middle];
            arrayToPartition = sortedX;
          }
          for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
            if (comparator.compare(arrayToPartition[i], middlePoint) < 0) {
              newSortedRange[position1++] = arrayToPartition[i];
            } else {
              newSortedRange[position2++] = arrayToPartition[i];
            }
          }
          // Copy the range [pend, last)
          System.arraycopy(newSortedRange, currentPart.pStart, arrayToPartition, currentPart.pStart, currentPart.pEnd - currentPart.pStart);
          toPartition.push(null); // An indicator of a merge needed
          // Create upper partition
          toPartition.push(new Part(currentPart.pStart, middle));
          // Create lower partition
          toPartition.push(new Part(middle, currentPart.pEnd));
        }
      }
    }

    if (toMerge.size() != 1)
      throw new RuntimeException("Expected exactly one final answer but found " + toMerge.size());

    return toMerge.pop();
  }
}
