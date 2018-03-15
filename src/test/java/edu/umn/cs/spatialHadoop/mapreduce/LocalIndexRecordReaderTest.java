package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
      final double[][] coords = readFile("src/test/resources/test.points");
      RTreeGuttman rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(coords[0], coords[1]);

      Configuration conf = new Configuration();
      FileSystem fs = outPath.getFileSystem(conf);
      FSDataOutputStream out = fs.create(outPath);
      rtree.write(out, new RTreeGuttman.Serializer() {
        @Override
        public int serialize(DataOutput out, int iObject) throws IOException {
          String line = coords[0][iObject] + "," + coords[1][iObject]+"\n";
          byte[] data = line.getBytes();
          out.write(data);
          return data.length;
        }
      });

      // Index the second file
      final double[][] coords2 = readFile("src/test/resources/test111.points");
      rtree = new RTreeGuttman(4, 8);
      rtree.initializeFromPoints(coords2[0], coords2[1]);

      rtree.write(out, new RTreeGuttman.Serializer() {
        @Override
        public int serialize(DataOutput out, int iObject) throws IOException {
          String line = coords2[0][iObject] + "," + coords2[1][iObject]+"\n";
          byte[] data = line.getBytes();
          out.write(data);
          return data.length;
        }
      });
      out.close();

      // Now search the concatenated file using LocalIndexRecordReader
      OperationsParams.setShape(conf, "shape", new Point());
      LocalIndexRecordReader<Point> lindex =
          new LocalIndexRecordReader<Point>(RRStarLocalIndex.class);
      lindex.initialize(new FileSplit(outPath, 0, fs.getFileStatus(outPath).getLen(), new String[0]), conf);
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
