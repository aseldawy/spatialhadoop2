package edu.umn.cs.spatialHadoop;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public abstract class BaseTest extends TestCase {

  public BaseTest(String name) {
    super(name);
  }

  public BaseTest() {}

  /**A scratch area used to do all the tests which gets wiped at the end*/
  protected static final Path scratchPath = new Path("testfiles");

  @Override
  protected void tearDown() throws Exception {
    OperationsParams params = new OperationsParams();
    FileSystem fs = scratchPath.getFileSystem(params);
    fs.delete(scratchPath, true);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    OperationsParams params = new OperationsParams();
    FileSystem fs = scratchPath.getFileSystem(params);
    if (fs.exists(scratchPath))
      fs.delete(scratchPath, true);
    if (!fs.exists(scratchPath))
      fs.mkdirs(scratchPath);
  }

  /**
   * Reads a csv file that contains coordinates
   * @param fileName
   * @return
   * @throws IOException
   */
  public static double[][] readFile(String fileName) throws IOException {
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
