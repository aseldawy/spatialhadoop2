package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements Dwyer's Algorithm for Delaunay triangulation as described in
 * Rex A. Dwyer, A Faster Divide-and-Conquer Algorithm for Constructing
 * Delaunay Triangulations. Algorithmica 2: 137-151 (1987)
 * http://dx.doi.org/10.1007/BF01840356
 * Created by Ahmed Eldawy on 12/15/16.
 */
public class DwyersAlgorithm extends GSDTAlgorithm {
  /**The grid used to partition records as defined in Dwyer's algorithm*/
  GridInfo grid;

  public <P extends Point> DwyersAlgorithm(final P[] points, Progressable progress) {
    super(points, progress);
  }

  public DwyersAlgorithm(Triangulation[] ts, Progressable progress) {
    super(ts, progress);
  }

  /**
   * Sort points by grid cell ID according to Dwyer's algorithm. Points within
   * each cell are sorted by x in accordance to Guibas and Stolfi's algorithm
   * as it will be applied within each cell.
   */
  protected void sortPointsByGrid() {
    int gridSize = (int) Math.ceil(Math.sqrt(points.length / (Math.log(points.length) / Math.log(2))));
    grid = new GridInfo(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Point p : points)
      grid.expand(p);
    grid.rows = grid.columns = gridSize;

    // Sort points by their containing cell
    QuickSort quickSort = new QuickSort();
    IndexedSortable gridSorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        int cellI = grid.getOverlappingCell(points[i].x, points[i].y);
        int cellJ = grid.getOverlappingCell(points[j].x, points[j].y);
        if (cellI != cellJ)
          return cellI - cellJ;
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
    quickSort.sort(gridSorter, 0, points.length);
  }

  /**
   * Compute the triangulation using Dwyer's algorithm. It works in the following
   * steps.
   * 1. Guibas and Stolfi's algorithm is applied within each cell.
   * 2. Cells within each row are merged.
   * 3. The partial answers in rows are merged.
   * @param start
   * @param end
   * @return
   */
  @Override
  protected IntermediateTriangulation computeTriangulation(int start, int end) {
    // Sort all points according to Dwyer's grid so that all points within each
    // grid are sorted by x-coordinate.
    sortPointsByGrid();

    // Fill in some auxiliary data structures to speed up the computation
    for (int i = start; i < end; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
    }

    // Dwyer's algorithm runs in three phases.
    // Phase I: Apply Guibas and Stolfi's algorithm in each cell
    // Phase II: Merge answers horizontally in all cells in each row
    // Phase III: Merge answers vertically in all rows
    List<IntermediateTriangulation> partialAnswers = new ArrayList<IntermediateTriangulation>();

    // Phase I:
    int i1 = start;
    while (i1 < end) {
      int cellID = grid.getOverlappingCell(xs[i1], ys[i1]);
      int i2 = i1 + 1;
      while (i2 < end && grid.getOverlappingCell(xs[i2], ys[i2]) == cellID)
        i2++;

      if (i2 - i1 == 1) {
        // One point. Cannot apply the Guibas Stolfi's algorithm from parent,
        // merge with the next cell as long as it is within the same row.
        int nextCellID = grid.getOverlappingCell(xs[i2], ys[i2]);
        if (cellID / grid.columns == nextCellID / grid.columns) {
          // Both points in the same row
          i2 = i2 + 1;
          while (i2 < end && grid.getOverlappingCell(xs[i2], ys[i2]) == cellID)
            i2++;
        } else {
          throw new RuntimeException("Cannot handle this situation");
        }
      }
      partialAnswers.add(super.computeTriangulation(i1, i2));
      i1 = i2;
    }

    // Phase II:
    while (partialAnswers.size() > 1) {
      int i, j;
      if (partialAnswers.size() == 2) {
        // If there are only two triangulations, merge them directly
        i = 0; j = 1;
      } else {
        // If there are three or more triangulations, skip the last one and try
        // to merge the two before it if they are in the same row
        i = partialAnswers.size() - 3;
        j = partialAnswers.size() - 2;

        Point pi = points[partialAnswers.get(i).site1];
        int cellI = grid.getOverlappingCell(pi.x, pi.y);
        Point pj = points[partialAnswers.get(j).site1];
        int cellJ = grid.getOverlappingCell(pj.x, pj.y);

        if (cellI / grid.columns != cellJ / grid.columns) {
          // If the two are from different rows, merge the last two triangulations
          i = partialAnswers.size() - 2;
          j = partialAnswers.size() - 1;
        }
      }
      // Merge the two triangulations at i and j
      IntermediateTriangulation ti = partialAnswers.get(i);
      IntermediateTriangulation tj = partialAnswers.get(j);
      IntermediateTriangulation result = merge(ti, tj);
      partialAnswers.remove(j);
      partialAnswers.set(i, result);
    }

    return partialAnswers.get(0);
  }
}
