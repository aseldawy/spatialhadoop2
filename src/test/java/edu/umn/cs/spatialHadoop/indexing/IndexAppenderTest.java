package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class IndexAppenderTest extends TestCase {

  protected Path indexPath = new Path("testindex");

  @Override
  protected void tearDown() throws Exception {
    OperationsParams params = new OperationsParams();
    FileSystem fs = indexPath.getFileSystem(params);
    fs.delete(indexPath, true);
  }

  public void testAppendToAnEmptyIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = indexPath.getFileSystem(params);
      outFS.delete(indexPath, true);
      outFS.deleteOnExit(indexPath);
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "rtree");
      IndexAppender.append(inPath, indexPath, params);
      assertTrue(outFS.exists(indexPath));
      assertTrue(outFS.exists(new Path(indexPath, "_master.rstar")));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void testAppendToAnExistingIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = indexPath.getFileSystem(params);
      outFS.delete(indexPath, true);
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "str");
      // Create the initial index
      Indexer.index(inPath, indexPath, params);

      // Append a second file to it
      inPath = new Path("src/test/resources/test2.points");
      IndexAppender.append(inPath, indexPath, params);
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(outFS, indexPath);
      for (Partition p : gindex) {
        assertEquals(33, p.recordCount);
        Path datafile = new Path(indexPath, p.filename);
        assertEquals(p.size, outFS.getFileStatus(datafile).getLen());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }

  public void testShouldInferLocalIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = indexPath.getFileSystem(params);
      outFS.delete(indexPath, true);
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "rtree");
      // Create the initial index
      Indexer.index(inPath, indexPath, params);

      // Append a second file to it
      inPath = new Path("src/test/resources/test2.points");
      params = new OperationsParams();
      params.setClass("shape", Point.class, Shape.class);
      IndexAppender.append(inPath, indexPath, params);

      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(outFS, indexPath);
      for (Partition p : gindex) {
        assertEquals(33, p.recordCount);
        Path datafile = new Path(indexPath, p.filename);
        long size = RangeQuery.rangeQueryLocal(datafile, new Rectangle(0, 0, 1000, 1000),
            new Point(), params, null);
        assertEquals(33L, size);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }
}