package edu.umn.cs.spatialHadoop.delaunay;

import edu.umn.cs.spatialHadoop.core.Point;
import org.apache.hadoop.util.Progressable;

/**
 * Implements Dwyer's Algorithm for Delaunay triangulation as described in
 * Rex A. Dwyer, A Faster Divide-and-Conquer Algorithm for Constructing
 * Delaunay Triangulations. Algorithmica 2: 137-151 (1987)
 * http://dx.doi.org/10.1007/BF01840356
 * Created by Ahmed Eldawy on 12/15/16.
 */
public class DwyersAlgorithm extends GSDTAlgorithm {

  public <P extends Point> DwyersAlgorithm(P[] points, Progressable progress) {
    int gridsize = Math.sqrt(points.length / (Math.log(points.length) / Math.log(2)));
  }

  public DwyersAlgorithm(Triangulation[] ts, Progressable progress) {
    super(ts, progress);
  }
}
