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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;

/**
 * Unit test for the utility class {@link Head}.
 */
public class LocalIndexRecordReaderTest extends TestCase {

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

  Path outPath = new Path("src/test/resources/temp_out");

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    // Clean up temporary file
    FileSystem outFS = outPath.getFileSystem(new Configuration());
    outFS.deleteOnExit(outPath);
  }


  public void testReadingConcatenatedLocalIndexes() {
    try {
      // Index the first file
      OperationsParams params = new OperationsParams();
      Path concatFile = new Path("concat.rtree");
      FileSystem fs = concatFile.getFileSystem(params);
      FSDataOutputStream out = fs.create(concatFile, true);
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);

      File[] inputFiles = new File[] {
          new File("src/test/resources/test.points"),
          new File("src/test/resources/test111.points"),
      };

      for (File file : inputFiles) {
        String tempRtreeFile = "temp.rtree";
        localIndexer.buildLocalIndex(file, new Path(tempRtreeFile), new Point());
        InputStream in = new FileInputStream(tempRtreeFile);
        IOUtils.copyBytes(in, out, params, false);
        in.close();
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
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
  }

  private double[][] readFile(String fileName) throws IOException {
    FileReader testPointsIn = new FileReader(fileName);
    char[] buffer = new char[(int) new File(fileName).length()];
    testPointsIn.read(buffer);
    testPointsIn.close();

    String[] lines = new String(buffer).split("\\s");
    int numDimensions = lines[0].split(",").length;
    double[][] coords = new double[numDimensions][lines.length];

    for (int iLine = 0; iLine < lines.length; iLine++) {
      String[] parts = lines[iLine].split(",");
      for (int iDim = 0; iDim < parts.length; iDim++)
        coords[iDim][iLine] = Double.parseDouble(parts[iDim]);
    }
    return coords;
  }

}
