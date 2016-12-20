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
      int pstart;
      int pend;

      Part(int start, int end) {
        this.pstart = start;
        this.pend = end;
      }

      @Override
      public String toString() {
        return "Partition: "+ pstart +","+ pend;
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
        LOG.info(String.format("%d to partition, %d to merge", toPartition.size(), toMerge.size()));
      }
      Part currentPart = toPartition.pop();
      if (currentPart == null) {
        // Merge the top two triangulations
        IntermediateTriangulation partial1 = toMerge.pop();
        IntermediateTriangulation partial2 = toMerge.pop();
        if (toMerge.isEmpty() && toPartition.isEmpty()) {
          // Last step
          partial1.draw();
          partial2.draw();
        }
        IntermediateTriangulation merged = merge(partial1, partial2);
        if (toMerge.isEmpty() && toPartition.isEmpty()) {
          // Last step
          merged.draw();
        }
        toMerge.push(merged);
      } else if (currentPart.pend - currentPart.pstart == 4) {
        xs[currentPart.pstart] = points[currentPart.pstart].x;
        ys[currentPart.pstart] = points[currentPart.pstart].y;
        xs[currentPart.pstart +1] = points[currentPart.pstart +1].x;
        ys[currentPart.pstart +1] = points[currentPart.pstart +1].y;
        xs[currentPart.pstart +2] = points[currentPart.pstart +2].x;
        ys[currentPart.pstart +2] = points[currentPart.pstart +2].y;
        xs[currentPart.pstart +3] = points[currentPart.pstart +3].x;
        ys[currentPart.pstart +3] = points[currentPart.pstart +3].y;
        // Compute DT for every two points
        toPartition.push(null);
        toMerge.push(new IntermediateTriangulation(currentPart.pstart + 2, currentPart.pstart + 3));
        toMerge.push(new IntermediateTriangulation(currentPart.pstart, currentPart.pstart + 1));
      } else if (currentPart.pend - currentPart.pstart == 3) {
        xs[currentPart.pstart] = points[currentPart.pstart].x;
        ys[currentPart.pstart] = points[currentPart.pstart].y;
        xs[currentPart.pstart +1] = points[currentPart.pstart +1].x;
        ys[currentPart.pstart +1] = points[currentPart.pstart +1].y;
        xs[currentPart.pstart +2] = points[currentPart.pstart +2].x;
        ys[currentPart.pstart +2] = points[currentPart.pstart +2].y;
        if (xs[currentPart.pstart] == xs[currentPart.pstart +1] &&
            xs[currentPart.pstart +1] == xs[currentPart.pstart +2]) {
          System.out.printf("A7aaaaaaa! Three colinear points (%d, %d, %d)\n", currentPart.pstart, currentPart.pstart+1, currentPart.pstart+2);
        }
        // Compute for three points
        toMerge.push(new IntermediateTriangulation(currentPart.pstart, currentPart.pstart + 1, currentPart.pstart + 2));
      } else if (currentPart.pend - currentPart.pstart == 2) {
        xs[currentPart.pstart] = points[currentPart.pstart].x;
        ys[currentPart.pstart] = points[currentPart.pstart].y;
        xs[currentPart.pstart +1] = points[currentPart.pstart +1].x;
        ys[currentPart.pstart +1] = points[currentPart.pstart +1].y;
        // Two points, connect with a line
        toMerge.push(new IntermediateTriangulation(currentPart.pstart, currentPart.pstart + 1));
      } else {
        // Further partition into two along the longer dimension
        double width = sortedX[currentPart.pend -1].x - sortedX[currentPart.pstart].x;
        double height = sortedY[currentPart.pend -1].y - sortedY[currentPart.pstart].y;
        if (width > height) {
          int middle = (currentPart.pstart + currentPart.pend) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedY list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.pend - currentPart.pstart];
            // Copy the range [pstart, pend)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            for (int i = currentPart.pstart; i < currentPart.pend; i++)
              if (sortedY[i].x < sortedX[middle].x)
                size_1++;
            if ((middle - currentPart.pstart) - size_1 < (currentPart.pend - currentPart.pstart) * 1 / 10) {
              middle = currentPart.pstart + size_1;
              // A clean situation, only one point at the middle or all points
              // at the middle line go to one partition
              int position1 = 0;
              int position2 = middle - currentPart.pstart;
              for (int i = currentPart.pstart; i < currentPart.pend; i++) {
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
              int position2 = middle - currentPart.pstart;
              for (int i = currentPart.pstart; i < currentPart.pend; i++) {
                // Search for the position of that object as compared to the middle object
                int position = Arrays.binarySearch(sortedX, currentPart.pstart, currentPart.pend, sortedY[i], comparatorX);
                if (position < middle)
                  newSortedRange[position1++] = sortedY[i];
                else
                  newSortedRange[position2++] = sortedY[i];
              }
            }
            // Copy the range [pend, last)
            System.arraycopy(newSortedRange, 0, sortedY, currentPart.pstart, currentPart.pend - currentPart.pstart);
          }

          // Create left partition
          toPartition.push(new Part(currentPart.pstart, middle));
          // Create right partition
          toPartition.push(new Part(middle, currentPart.pend));
        } else {
          // Partition along the Y-axis
          int middle = (currentPart.pstart + currentPart.pend) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedX list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.pend - currentPart.pstart];
            // Copy the range [pstart, pend)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            if ((middle - currentPart.pstart) - size_1 < (currentPart.pend - currentPart.pstart) * 1 / 10) {
              middle = currentPart.pstart + size_1;
              for (int i = currentPart.pstart; i < currentPart.pend; i++)
                if (sortedX[i].y < sortedY[middle].y)
                  size_1++;
              int position1 = 0;
              int position2 = size_1;
              for (int i = currentPart.pstart; i < currentPart.pend; i++) {
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
              int position2 = middle - currentPart.pstart;
              for (int i = currentPart.pstart; i < currentPart.pend; i++) {
                // Search for the position of that object as compared to the middle object
                int position = Arrays.binarySearch(sortedY, currentPart.pstart, currentPart.pend, sortedX[i], comparatorY);
                if (position < middle)
                  newSortedRange[position1++] = sortedX[i];
                else
                  newSortedRange[position2++] = sortedX[i];
              }
            }
            // Copy the range [pend, last)
            System.arraycopy(newSortedRange, 0, sortedX, currentPart.pstart, currentPart.pend - currentPart.pstart);
          }
          // Create upper partition
          toPartition.push(new Part(currentPart.pstart, middle));
          // Create lower partition
          toPartition.push(new Part(middle, currentPart.pend));
        }
      }
    }

    if (toMerge.size() != 1)
      throw new RuntimeException("Expected exactly one final answer but found " + toMerge.size());

    return toMerge.pop();
  }
}
