package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Unit test for the index construction operation
 */
public class IndexerTest extends TestCase {

  Path outPath = new Path("src/test/resources/temp_out");

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public IndexerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(IndexerTest.class);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    // Clean up temporary file
    FileSystem outFS = outPath.getFileSystem(new Configuration());
    outFS.delete(outPath, true);
  }

  public void testEmptyGeometries() {
    // Should not crash with an input file that contains empty geometries
    try {
      FileSystem outFS = outPath.getFileSystem(new Configuration());
      outFS.delete(outPath, true);

      Path inPath = new Path("src/test/resources/polys.osm");
      // Test MapReduce implementation
      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.set("sindex", "rtree");
      Indexer.index(inPath, outPath, params);

      // Test local implementation
      outFS.delete(outPath, true);
      params.setBoolean("local", true);
      Indexer.index(inPath, outPath, params);

      // Test local implementation with disjoint partition
      outFS.delete(outPath, true);
      params.setBoolean("local", true);
      params.set("sindex", "str+");
      Indexer.index(inPath, outPath, params);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error indexing the file");
    }
  }

  public void testCommonIndexes() {
    String[] indexes = {"grid", "str", "str+", "rtree", "r+tree", "hilbert", "zcurve", "quadtree"};
    for (String index : indexes) {
      try {
        FileSystem outFS = outPath.getFileSystem(new Configuration());
        outFS.delete(outPath, true);
        Path inPath = new Path("src/test/resources/polys.osm");

        OperationsParams params = new OperationsParams();
        params.setBoolean("local", true);
        params.setClass("shape", OSMPolygon.class, Shape.class);
        params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
        params.set("sindex", index);
        Indexer.index(inPath, outPath, params);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Error indexing the file with "+index);
      }
    }
  }

  public void testShouldWorkWithoutSIndexInMapReduceMode() {
    try {
      FileSystem outFS = outPath.getFileSystem(new Configuration());
      outFS.delete(outPath, true);
      Path inPath = new Path("src/test/resources/polys.osm");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.set("gindex", "rstar");
      Indexer.index(inPath, outPath, params);
      assertTrue("Global index file not found!",
          outFS.exists(new Path(outPath, "_master.rstar")));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error indexing the file with an rtree global index");
    }
  }

  public void testShouldWorkWithoutSIndexInLocalMode() {
    try {
      FileSystem outFS = outPath.getFileSystem(new Configuration());
      outFS.delete(outPath, true);
      Path inPath = new Path("src/test/resources/polys.osm");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", true);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.set("gindex", "rstar");
      Indexer.index(inPath, outPath, params);
      assertTrue("Global index file not found!",
          outFS.exists(new Path(outPath, "_master.rstar")));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error indexing the file with an rtree global index");
    }
  }


  public void testCreatePartitioner() {
    // Should not crash with an input file that contains empty geomtries
    try {
      Path inPath = new Path("src/test/resources/polys.osm");
      Path outPath = new Path("temp");

      OperationsParams params = new OperationsParams();
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);

      Partitioner p = Indexer.initializeGlobalIndex(inPath, outPath, params, RStarTreePartitioner.class);
      assertTrue(p instanceof RStarTreePartitioner);

      OperationsParams.setShape(params, "mbr", new Rectangle(0,0,10,10));
      p = Indexer.initializeGlobalIndex(inPath, outPath, params, GridPartitioner.class);
      assertTrue(p instanceof GridPartitioner);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error working with the test file");
    }
  }

  public void testLocalIndexing() {
    try {
      Path inPath = new Path("src/test/resources/test.points");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setClass("shape", Point.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.setClass("gindex", RStarTreePartitioner.class, Partitioner.class);
      params.setClass("lindex", RRStarLocalIndex.class, LocalIndex.class);
      Indexer.index(inPath, outPath, params);

      // Test with range query
      long resultSize = RangeQuery.rangeQueryLocal(outPath,
          new Rectangle(0, 0, 5, 5), new Point(), params, null);
      assertEquals(2, resultSize);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error while building the index");
    }
  }

  public void testWorkWithSmallFiles() {
    assertEquals(1, GridPartitioner.class.getAnnotations().length);
    try {
      FileSystem outFS = outPath.getFileSystem(new Configuration());
      outFS.delete(outPath, true);
      Path inPath = new Path("src/test/resources/test.points");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setClass("shape", Point.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.set("sindex", "grid");
      Job job = Indexer.index(inPath, outPath, params);
      assertTrue("Job failed!", job.isSuccessful());
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error working with the test file");
    } catch (InterruptedException e) {
      fail("Error running the job");
    } catch (ClassNotFoundException e) {
      fail("Could not create the shape");
    }
  }

  public void testRepartitioner() {
    Path refPath = new Path(outPath.getParent(), "refpath");
    Configuration conf = new Configuration();

    try {
      FileSystem outFS = refPath.getFileSystem(conf);
      outFS.delete(refPath, true);

      outFS.mkdirs(refPath);
      FSDataOutputStream out = outFS.create(new Path(refPath, "_master.rstar"));
      PrintStream ps = new PrintStream(out);
      Partition[] fakePartitions = {
          new Partition("data1", new CellInfo(1, 0, 0, 10, 10)),
          new Partition("data2", new CellInfo(2, 20, 5, 30, 50)),
      };
      for (Partition p : fakePartitions)
        ps.println(p.toText(new Text("")));
      ps.close();
      // Create a fake files
      outFS.create(new Path(refPath, "data1")).close();
      outFS.create(new Path(refPath, "data2")).close();

      Partitioner p = Indexer.initializeRepartition(refPath, conf);

      assertTrue("Invalid partitioner created", p instanceof CellPartitioner);
      assertEquals(2, p.getPartitionCount());

      // Run the repartition job
      OperationsParams params = new OperationsParams();
      params.set("shape", "rect");
      outFS.delete(outPath, true);
      Indexer.repartition(new Path("src/test/resources/test.rect"), outPath, refPath, params);
      assertTrue("Output file not created", outFS.exists(outPath));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error while running test!");
    } finally {

    }
  }
}
