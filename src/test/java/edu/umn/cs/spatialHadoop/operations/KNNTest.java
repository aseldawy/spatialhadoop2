package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Unit test for {@link LocalSampler} class.
 */
public class KNNTest extends BaseTest {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public KNNTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(KNNTest.class);
  }
  
  public void testKNNWithLocalIndex() {
    try {
      Path inputFile = new Path("src/test/resources/test.points");
      Path outPath = new Path(scratchPath, "tempout");
      OperationsParams params = new OperationsParams();
      params.set("shape", "point");
      params.set("sindex", "rtree");
      params.setBoolean("local", false);
      outPath.getFileSystem(params).delete(outPath, true);
      Indexer.index(inputFile, outPath, params);

      // Now run a knn local query on the index
      params.set("point", "0,0");
      params.setInt("k", 3);
      params.setBoolean("local", true);
      KNN.knn(outPath, null, params);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error while indexing");
    }
  }
}
