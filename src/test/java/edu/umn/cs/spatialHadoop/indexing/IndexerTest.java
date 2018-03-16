package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileNotFoundException;
import java.io.IOException;

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

  public void testShouldWorkWithoutSIndex() {
    try {
      FileSystem outFS = outPath.getFileSystem(new Configuration());
      outFS.delete(outPath, true);
      Path inPath = new Path("src/test/resources/polys.osm");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", true);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
      params.set("gindex", "rstree");
      Indexer.index(inPath, outPath, params);
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
    // Should not crash with an input file that contains empty geomtries
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
}
