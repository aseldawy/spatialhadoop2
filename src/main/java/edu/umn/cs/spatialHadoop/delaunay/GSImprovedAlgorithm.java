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
    final Point[] sortedX = this.points;
    final Point[] sortedY = this.points.clone();


    // Sort all points and record the order of merging
    class Part {
      int start;
      int end;

      Part(int start, int end) {
        this.start = start;
        this.end = end;
      }
    };

    QuickSort quickSort = new QuickSort();
    IndexedSortable xSorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        if (sortedX[i].x < sortedX[j].x)
          return -1;
        if (sortedX[i].x > sortedX[j].x)
          return 1;
        if (sortedX[i].y < sortedX[j].y)
          return -1;
        if (sortedX[i].y > sortedX[j].y)
          return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        Point t = sortedX[i];
        sortedX[i] = sortedX[j];
        sortedX[j] = t;
      }
    };
    IndexedSortable ySorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        if (sortedY[i].y < sortedY[j].y)
          return -1;
        if (sortedY[i].y > sortedY[j].y)
          return 1;
        if (sortedY[i].x < sortedY[j].x)
          return -1;
        if (sortedY[i].x > sortedY[j].x)
          return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        Point t = sortedY[i];
        sortedY[i] = sortedY[j];
        sortedY[j] = t;
      }
    };

    // Sort all points by X in one list, and by Y in the other list
    quickSort.sort(xSorter, start, end);
    quickSort.sort(ySorter, start, end);

    // Partitions to be sorted, a null entry indicates that it is time to merge
    // the top two entries in the triangulations to be merged
    Stack<Part> toPartition = new Stack<Part>();
    // Triangulations to merge
    Stack<IntermediateTriangulation> toMerge = new Stack<IntermediateTriangulation>();

    // Compute the MBR of the input
    Part topPart = new Part(start, end);
    toPartition.push(topPart);

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
        // Compute the points directly using the regular Guibas and Stolfi's algorithm
        IntermediateTriangulation partialAnswer = super.computeTriangulation(currentPart.start, currentPart.end);
        toMerge.push(partialAnswer);
      } else {
        // Further partition into two along the longer dimension
        double width = sortedX[currentPart.end-1].x - sortedX[currentPart.start].x;
        double height = sortedY[currentPart.end-1].x - sortedY[currentPart.start].x;
        if (width > height) {
          int middle = (currentPart.start + currentPart.end) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedY list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.end - currentPart.start];
            // Copy the range [start, end)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            for (int i = currentPart.start; i < currentPart.end; i++)
              if (sortedY[i].x < sortedX[middle].x)
                size_1++;
            int position1 = 0;
            int position2 = size_1;
            for (int i = currentPart.start; i < currentPart.end; i++) {
              if (sortedY[i].x < sortedX[middle].x)
                newSortedRange[position1++] = sortedY[i];
              else
                newSortedRange[position2++] = sortedY[i];
            }
            // Copy the range [end, last)
            System.arraycopy(newSortedRange, 0, sortedY, currentPart.start, currentPart.end - currentPart.start);
          }

          // Create left partition
          Part left = new Part(currentPart.start, middle);
          toPartition.push(left);
          // Create right partition
          Part right = new Part(middle, currentPart.end);
          toPartition.push(right);
        } else {
          // Partition along the Y-axis
          int middle = (currentPart.start + currentPart.end) / 2;
          toPartition.push(null); // An indicator of a merge needed
          // TODO Split the sortedX list around the middle
          {
            // For now, use a naive way to ensure that the algorithm works
            Point[] newSortedRange = new Point[currentPart.end - currentPart.start];
            // Copy the range [start, end)
            // Count number of points in the first part (left to middle.x)
            int size_1 = 0;
            for (int i = currentPart.start; i < currentPart.end; i++)
              if (sortedX[i].y < sortedY[middle].y)
                size_1++;
            int position1 = 0;
            int position2 = size_1;
            for (int i = currentPart.start; i < currentPart.end; i++) {
              if (sortedX[i].y < sortedY[middle].y)
                newSortedRange[position1++] = sortedY[i];
              else
                newSortedRange[position2++] = sortedY[i];
            }
            // Copy the range [end, last)
            System.arraycopy(newSortedRange, 0, sortedY, currentPart.start, currentPart.end - currentPart.start);
          }
          // Create upper partition
          Part upper = new Part(currentPart.start, middle);
          toPartition.push(upper);
          // Create lower partition
          Part lower = new Part(middle, currentPart.end);
          toPartition.push(lower);
        }
      }
    }

    if (toMerge.size() != 1)
      throw new RuntimeException("Expected exactly one file answer but found " + toMerge.size());

    return toMerge.pop();
  }
}
