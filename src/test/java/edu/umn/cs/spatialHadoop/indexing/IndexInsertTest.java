package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.TestHelper;
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.util.MetadataUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

public class IndexInsertTest extends BaseTest {

  public void testAppendToAnEmptyIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = scratchPath.getFileSystem(params);
      outFS.delete(scratchPath, true);
      outFS.deleteOnExit(scratchPath);
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "rtree");
      IndexInsert.append(inPath, scratchPath, params);
      assertTrue(outFS.exists(scratchPath));
      assertTrue(outFS.exists(new Path(scratchPath, "_master.rstar")));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testAppendToAnExistingIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "str");
      // Create the initial index
      Indexer.index(inPath, scratchPath, params);

      // Append a second file to it
      inPath = new Path("src/test/resources/test2.points");
      IndexInsert.append(inPath, scratchPath, params);
      FileSystem fs = scratchPath.getFileSystem(params);
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, scratchPath);
      Partition merged = null;
      for (Partition p : gindex) {
        if (merged == null)
          merged = p.clone();
        else
          merged.expand(p);
        Path datafile = new Path(scratchPath, p.filename);
        assertEquals(p.size, fs.getFileStatus(datafile).getLen());
      }
      assertEquals(33, merged.recordCount);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testAppendToAnExistingIndexWithSeveralPartitions() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      Path indexPath = new Path(scratchPath, "testindex");
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
      IndexInsert.append(inPath, indexPath, params);
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
      throw new RuntimeException(e);
    }
  }

  public void testShouldInferLocalIndex() throws IOException {
    try {
      Path inPath = new Path("src/test/resources/test.points");
      OperationsParams params = new OperationsParams();
      FileSystem outFS = scratchPath.getFileSystem(params);
      outFS.delete(scratchPath, true);
      params.setClass("shape", Point.class, Shape.class);
      params.set("sindex", "rtree");
      // Create the initial index
      Indexer.index(inPath, scratchPath, params);

      // Append a second file to it
      inPath = new Path("src/test/resources/test2.points");
      params = new OperationsParams();
      params.setClass("shape", Point.class, Shape.class);
      IndexInsert.append(inPath, scratchPath, params);

      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(outFS, scratchPath);
      for (Partition p : gindex) {
        assertEquals(33, p.recordCount);
        Path datafile = new Path(scratchPath, p.filename);
        long size = RangeQuery.rangeQueryLocal(datafile, new Rectangle(0, 0, 1000, 1000),
            new Point(), params, null);
        assertEquals(33L, size);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testReorganize() {
    Path dataPath = new Path(scratchPath, "data");
    Path indexPath = new Path(scratchPath, "indexed");
    OperationsParams params = new OperationsParams();
    try {
      FileSystem fs = dataPath.getFileSystem(params);
      // 1- Generate a file and index it
      TestHelper.generateFile(dataPath.toString(), Point.class,
          new Rectangle(0,0,1,1), 2 * 1024 * 1024, params);
      long fileSize1 = fs.getFileStatus(dataPath).getLen();
      // Set the parameters to partition the file into at least two partitions
      params.set("gindex", "rstar");
      params.set("lindex", "rrstar");
      params.setLong("fs.local.block.size", 1024 * 1024);
      params.setFloat("ratio", 1.0f);
      Indexer.index(dataPath, indexPath, params);

      // 2- Generate another file and append it
      // Set a budget that is enough to partition at least one partition but not all partitions
      params.setLong("budget", 1024 * 1024);
      TestHelper.generateFile(dataPath.toString(), Point.class,
          new Rectangle(5,5,10,10), 2 * 1024 * 1024, params);
      long fileSize2 = fs.getFileStatus(dataPath).getLen();
      IndexInsert.addToIndex(dataPath, indexPath, params);
      // Assert that the new path contain all the data
      ArrayList<Partition> ps = MetadataUtil.getPartitions(indexPath, params);
      Partition all = new Partition();
      all.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      all.size = all.recordCount = 0;
      for (Partition p : ps)
        all.expand(p);
      assertEquals(fileSize1 + fileSize2, all.size);
      assertTrue("Partitions should be reorganized", ps.size() > 1);
      FileStatus[] dataFiles = fs.listStatus(indexPath, SpatialSite.NonHiddenFileFilter);
      assertEquals(ps.size(), dataFiles.length);

      // Make sure that the resulting index is still searchable
      long resultSize = RangeQuery.rangeQueryLocal(indexPath,
          new Rectangle(0, 0, 10, 10), new Point(), params, null);
      assertEquals(all.recordCount, resultSize);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}