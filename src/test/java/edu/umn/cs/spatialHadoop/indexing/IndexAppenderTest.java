package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.PrintStream;

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

  public void testAppendToAnExistingIndexWithSeveralPartitions() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      Path indexPath = new Path("testindex");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = indexPath.getFileSystem(params);

      // Create a fake existing index
      outFS.mkdirs(indexPath);
      FSDataOutputStream out = outFS.create(new Path(indexPath, "_master.rstar"));
      PrintStream ps = new PrintStream(out);
      Partition[] fakePartitions = {
          new Partition("data1", new CellInfo(1, 0, 0, 7, 15)),
          new Partition("data2", new CellInfo(2, 7, 0, 15, 15)),
      };
      for (Partition p : fakePartitions)
        ps.println(p.toText(new Text("")));
      ps.close();
      // Create fake empty files
      outFS.create(new Path(indexPath, "data1")).close();
      outFS.create(new Path(indexPath, "data2")).close();

      // Append a second file to it
      params.setClass("shape", Point.class, Shape.class);
      IndexAppender.append(inPath, indexPath, params);
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(outFS, indexPath);
      int count = 0;

      Partition merged = null;
      for (Partition p : gindex) {
        count++;
        if (merged == null)
          merged = p.clone();
        else
          merged.expand(p);
      }
      assertEquals(11, merged.recordCount);
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