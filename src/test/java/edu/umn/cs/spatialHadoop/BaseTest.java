package edu.umn.cs.spatialHadoop;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
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

  public static String[] readTextFile(String fileName) throws IOException {
    File f = new File(fileName);
    File[] files;
    if (f.isDirectory()) {
      files = f.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });
    } else {
      files = new File[] {new File(fileName)};
    }
    // Count number of lines first
    int numLines = 0;
    for (File file : files) {
      FileReader testPointsIn = new FileReader(file);
      char[] buffer = new char[(int) file.length()];
      testPointsIn.read(buffer);
      testPointsIn.close();
      int i = 0;
      int iLastLine = -1;
      while (i < buffer.length) {
        if (buffer[i] == '\n' || buffer[i] == '\r') {
          if (i-iLastLine > 1)
            numLines++;
          iLastLine = i;
        }
        i++;
      }
      if (i-iLastLine > 1)
        numLines++;
    }
    String[] results = new String[numLines];
    int numOutLines = 0;

    for (File file : files) {
      FileReader testPointsIn = new FileReader(file);
      char[] buffer = new char[(int) file.length()];
      testPointsIn.read(buffer);
      testPointsIn.close();

      if (buffer.length > 0) {
        String[] lines = new String(buffer).trim().split("[\\n\\r]+");
        System.arraycopy(lines, 0, results, numOutLines, lines.length);
        numOutLines += lines.length;
      }
    }
    return results;
  }

  /**
   * Reads a csv file that contains coordinates
   * @param fileName
   * @return
   * @throws IOException
   */
  public static double[][] readFile(String fileName) throws IOException {
    File f = new File(fileName);
    File[] files;
    if (f.isDirectory()) {
      files = f.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });
    } else {
      files = new File[] {new File(fileName)};
    }
    // Count number of lines first
    int numLines = 0;
    int numDimensions = -1;
    for (File file : files) {
      FileReader testPointsIn = new FileReader(file);
      char[] buffer = new char[(int) file.length()];
      testPointsIn.read(buffer);
      testPointsIn.close();
      int i = 0;
      int iLastLine = -1;
      while (i < buffer.length) {
        if (buffer[i] == '\n' || buffer[i] == '\r') {
          if (i-iLastLine > 1)
            numLines++;
          iLastLine = i;
        }
        i++;
      }
      if (i-iLastLine > 1)
        numLines++;
      if (numDimensions == -1) {
        i = 0;
        numDimensions = 1;
        while (i < buffer.length && buffer[i] != '\n') {
          if (buffer[i] == ',')
            numDimensions++;
          i++;
        }
      }
    }
    double[][] coords = new double[numDimensions][numLines];
    int numOutLines = 0;

    for (File file : files) {
      FileReader testPointsIn = new FileReader(file);
      char[] buffer = new char[(int) file.length()];
      testPointsIn.read(buffer);
      testPointsIn.close();

      String[] lines = new String(buffer).split("\\s");

      for (String line : lines) {
        String[] parts = line.split(",");
        for (int iDim = 0; iDim < parts.length; iDim++)
          coords[iDim][numOutLines] = Double.parseDouble(parts[iDim]);
        numOutLines++;
      }

    }
    return coords;
  }
}
