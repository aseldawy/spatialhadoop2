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
    Part topPart = new Part(rstart, rend);
    toPartition.push(topPart);
    long reportedTime = 0;

    while (!toPartition.isEmpty()) {
      if (progress != null)
        progress.progress();
      if (System.currentTimeMillis() - reportedTime > 1000) {
        reportedTime = System.currentTimeMillis();
        LOG.debug(String.format("%d to partition, %d to merge", toPartition.size(), toMerge.size()));
      }
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
        if (xs[currentPart.pStart] == xs[currentPart.pStart +1] &&
            xs[currentPart.pStart +1] == xs[currentPart.pStart +2]) {
          System.out.printf("A7aaaaaaa! Three colinear points (%d, %d, %d)\n", currentPart.pStart, currentPart.pStart +1, currentPart.pStart +2);
        }
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
        if (width > height) {
          int middle = (currentPart.pStart + currentPart.pEnd) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedY list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.pEnd - currentPart.pStart];
            // Copy the range [pStart, pend)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            for (int i = currentPart.pStart; i < currentPart.pEnd; i++)
              if (sortedY[i].x < sortedX[middle].x)
                size_1++;
            if ((middle - currentPart.pStart) - size_1 < (currentPart.pEnd - currentPart.pStart) * 1 / 10) {
              middle = currentPart.pStart + size_1;
              // A clean situation, only one point at the middle or all points
              // at the middle line go to one partition
              int position1 = 0;
              int position2 = middle - currentPart.pStart;
              for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
                if (sortedY[i].x < sortedX[middle].x)
                  newSortedRange[position1++] = sortedY[i];
                else
                  newSortedRange[position2++] = sortedY[i];
              }
            } else {
              // A degenerate case, more than one point at the middle line and
              // they need to go to different partitions
              // Apply a slower, more sophisticated algorithm
              int position1 = 0;
              int position2 = middle - currentPart.pStart;
              for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
                // Search for the position of that object as compared to the middle object
                int position = Arrays.binarySearch(sortedX, currentPart.pStart, currentPart.pEnd, sortedY[i], comparatorX);
                if (position < middle)
                  newSortedRange[position1++] = sortedY[i];
                else
                  newSortedRange[position2++] = sortedY[i];
              }
            }
            // Copy the range [pend, last)
            System.arraycopy(newSortedRange, 0, sortedY, currentPart.pStart, currentPart.pEnd - currentPart.pStart);
          }

          // Create left partition
          toPartition.push(new Part(currentPart.pStart, middle));
          // Create right partition
          toPartition.push(new Part(middle, currentPart.pEnd));
        } else {
          // Partition along the Y-axis
          int middle = (currentPart.pStart + currentPart.pEnd) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedX list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.pEnd - currentPart.pStart];
            // Copy the range [pStart, pend)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            if ((middle - currentPart.pStart) - size_1 < (currentPart.pEnd - currentPart.pStart) * 1 / 10) {
              middle = currentPart.pStart + size_1;
              for (int i = currentPart.pStart; i < currentPart.pEnd; i++)
                if (sortedX[i].y < sortedY[middle].y)
                  size_1++;
              int position1 = 0;
              int position2 = size_1;
              for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
                if (sortedX[i].y < sortedY[middle].y)
                  newSortedRange[position1++] = sortedX[i];
                else
                  newSortedRange[position2++] = sortedX[i];
              }
            } else {
              // A degenerate case, more than one point at the middle line and
              // they need to go to different partitions
              // Apply a slower, more sophisticated algorithm
              int position1 = 0;
              int position2 = middle - currentPart.pStart;
              for (int i = currentPart.pStart; i < currentPart.pEnd; i++) {
                // Search for the position of that object as compared to the middle object
                int position = Arrays.binarySearch(sortedY, currentPart.pStart, currentPart.pEnd, sortedX[i], comparatorY);
                if (position < middle)
                  newSortedRange[position1++] = sortedX[i];
                else
                  newSortedRange[position2++] = sortedX[i];
              }
            }
            // Copy the range [pend, last)
            System.arraycopy(newSortedRange, 0, sortedX, currentPart.pStart, currentPart.pEnd - currentPart.pStart);
          }
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
