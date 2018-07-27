package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of the R-tree partitioner based on a gray box implementation of the R-tree linear-time split
 * algorithm. It seems a little bit weird to extend the {@link AbstractRTreeBBPartitioner} rather than the
 * {@link AbstractRTreeGBPartitioner} for this gray box implementation. However, since the auxiliary data structure is
 * not supported by the graybox R-tree partitioner, we found that it is closer to the BB implementation in this manner.
 */
@Partitioner.GlobalIndexerMetadata(disjoint = true, extension = "rtreegb",
    requireSample = true)
public class RTreeGBPartitioner extends AbstractRTreeBBPartitioner {

  /**The minimum fraction to use when applying the linear-time split algorithm*/
  protected float fractionMinSplitSize;

  @Override
  public RTreeGuttman createRTree(int m, int M) {
    return null;
  }

  @Override
  public void setup(Configuration conf) {
    super.setup(conf);
    this.mMRatio = conf.getFloat("mMRatio", 0.95f);
    this.fractionMinSplitSize = conf.getFloat("fractionMinSplitSize", 0.0f);
  }

  @Override
  public void construct(Rectangle mbr, Point[] points, int capacity) {
    double[] xs = new double[points.length];
    double[] ys = new double[points.length];
    for (int i = 0; i < points.length; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
    }
    int M = capacity;
    int m = (int) Math.ceil(M * mMRatio);
    Rectangle[] partitions = RTreeGuttman.partitionPoints(xs, ys, m, M, fractionMinSplitSize);
    x1s = new double[partitions.length];
    y1s = new double[partitions.length];
    x2s = new double[partitions.length];
    y2s = new double[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      x1s[i] = partitions[i].x1;
      y1s[i] = partitions[i].y1;
      x2s[i] = partitions[i].x2;
      y2s[i] = partitions[i].y2;
    }
  }
}
