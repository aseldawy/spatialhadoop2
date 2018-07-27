package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.BaseTest;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;

/**
 * Unit test for the {@code LocalIndexRecordReader} class
 */
public class LocalIndexRecordReaderTest extends BaseTest {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public LocalIndexRecordReaderTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(LocalIndexRecordReaderTest.class);
  }

  public void testReadingConcatenatedLocalIndexes() {
    Path concatFile = new Path(scratchPath, "concat.rtree");
    try {
      // Index the first file
      OperationsParams params = new OperationsParams();
      FileSystem fs = concatFile.getFileSystem(params);
      FSDataOutputStream out = fs.create(concatFile, true);
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);

      File[] inputFiles = new File[] {
          new File("src/test/resources/test.points"),
          new File("src/test/resources/test111.points"),
      };

      for (File file : inputFiles) {
        Path tempFile = new Path(scratchPath, "temp.rrstar");
        localIndexer.buildLocalIndex(file, tempFile, new Point());
        InputStream in = fs.open(tempFile);
        IOUtils.copyBytes(in, out, params, false);
        in.close();
        fs.delete(tempFile, false);
      }
      out.close();

      // Now search the concatenated file using LocalIndexRecordReader
      OperationsParams.setShape(params, "shape", new Point());
      LocalIndexRecordReader<Point> lindex =
          new LocalIndexRecordReader<Point>(RRStarLocalIndex.class);
      lindex.initialize(new FileSplit(concatFile, 0, fs.getFileStatus(concatFile).getLen(), new String[0]), params);
      int count = 0;
      while (lindex.nextKeyValue()) {
        Iterable<? extends Point> points = lindex.getCurrentValue();
        for (Point p : points)
          count++;
      }
      assertEquals(11+111, count);
      assertEquals(1.0f, lindex.getProgress());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }

}
