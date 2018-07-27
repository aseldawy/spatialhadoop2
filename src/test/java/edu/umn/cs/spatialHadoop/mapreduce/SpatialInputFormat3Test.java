package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.TestHelper;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.indexing.RRStarLocalIndex;
import edu.umn.cs.spatialHadoop.indexing.RTreeGuttman;
import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.List;

/**
 * Unit test for the utility class {@link Head}.
 */
public class SpatialInputFormat3Test extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public SpatialInputFormat3Test(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(SpatialInputFormat3Test.class);
  }

  public void testShouldSplitTextFiles() {
    // Build an R-tree on a random data and store in a file
    Path randomFile = new Path("temp.points");
    try {
      final Rectangle mbr = new Rectangle(0, 0, 1000, 1000);
      // Generate a random file of points
      OperationsParams params = new OperationsParams();
      TestHelper.generateFile(randomFile.getName(), Point.class, mbr, 1024*1024, params);

      long inputSize = new File(randomFile.getName()).length();
      params.setLong(SpatialInputFormat3.SPLIT_MAXSIZE, inputSize * 2 / 3);
      Job job = Job.getInstance(params);
      SpatialInputFormat3.addInputPath(job, randomFile);
      List<InputSplit> splits = new SpatialInputFormat3().getSplits(job);
      assertEquals(2, splits.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }

  public void testShouldNotSplitLocallyIndexedFiles() {
    // Build an R-tree on a random data and store in a file
    Path randomFile = new Path("temp.points");
    Path rtreeFile = new Path("temp.rrstar");
    try {
      final Rectangle mbr = new Rectangle(0, 0, 1000, 1000);
      // Generate a random file of points
      OperationsParams params = new OperationsParams();
      TestHelper.generateFile(randomFile.getName(), Point.class, mbr, 1024*1024, params);

      // Locally index the file
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);
      localIndexer.buildLocalIndex(new File(randomFile.getName()), rtreeFile, new Point());

      long inputSize = new File(rtreeFile.getName()).length();
      params.setLong(SpatialInputFormat3.SPLIT_MAXSIZE, inputSize * 2 / 3);
      Job job = Job.getInstance(params);
      SpatialInputFormat3.addInputPath(job, rtreeFile);
      List<InputSplit> splits = new SpatialInputFormat3().getSplits(job);
      assertEquals(1, splits.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }
}
