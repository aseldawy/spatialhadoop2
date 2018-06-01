package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AbstractRTreeBBPartitionerTest extends BaseTest {

  public void testConstruct() throws IOException {
    double[][] coords = readFile("src/test/resources/test.points");
    Point[] points = new Point[coords[0].length];
    for (int i = 0; i < points.length; i++) {
      points[i] = new Point(coords[0][i], coords[1][i]);
    }
    AbstractRTreeBBPartitioner p = new AbstractRTreeBBPartitioner.RTreeGuttmanBBPartitioner();
    p.setup(new Configuration());
    p.construct(null, points, 4);
    assertTrue("Too few partitions", p.getPartitionCount() > 2);
    Set<Integer> partitions = new HashSet<Integer>();
    for (Point pt : points) {
      partitions.add(p.overlapPartition(pt));
    }
    assertEquals(p.getPartitionCount(), partitions.size());
  }

  public void testOverlapPartitionShouldChooseMinimalArea() {
    Rectangle[] partitions = { new Rectangle(0,0,4,4),
        new Rectangle(1,1,3,3)};
    AbstractRTreeBBPartitioner p = new AbstractRTreeBBPartitioner.RTreeGuttmanBBPartitioner();
    initializeBBPartitioner(p, partitions);
    assertEquals(0, p.overlapPartition(new Point(0.5, 0.5)));
    assertEquals(1, p.overlapPartition(new Point(2, 2)));
  }

  private void initializeBBPartitioner(AbstractRTreeBBPartitioner p, Rectangle[] partitions) {
    p.x1s = new double[partitions.length];
    p.y1s = new double[partitions.length];
    p.x2s = new double[partitions.length];
    p.y2s = new double[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      p.x1s[i] = partitions[i].x1;
      p.x2s[i] = partitions[i].x2;
      p.y1s[i] = partitions[i].y1;
      p.y2s[i] = partitions[i].y2;
    }
  }
}