package edu.umn.cs.spatialHadoop.visualization;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import edu.umn.cs.spatialHadoop.BaseTest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.OperationsParams;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class MultilevelPlotTest extends BaseTest {
  private static final Path inFileName = new Path(scratchPath, "test.rect");
  private static final Path outFileName = new Path(scratchPath, "test_pyramid");

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public MultilevelPlotTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(MultilevelPlotTest.class);
  }
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    // Create an input file
    Configuration conf = new Configuration();
    FileSystem fs = scratchPath.getFileSystem(conf);
    fs.mkdirs(scratchPath);
    PrintWriter pw = new PrintWriter(fs.create(inFileName));
    pw.println("0,0,0.5,0.5");
    pw.close();
  }

  public void testOneLevelLocal() {
    try {
      OperationsParams params = new OperationsParams();
      params.setBoolean("local", true);
      params.set("levels", "11..11");
      params.set("mbr", "0,0,2048,2048");
      params.set("shape", "rect");
      params.setBoolean("overwrite", true);
      params.setBoolean("vflip", false);
      
      MultilevelPlot.plot(new Path[] { inFileName },
          outFileName, GeometricPlot.GeometricRasterizer.class,
          params);

      File outPath = new File(outFileName.toString());
      String[] list = outPath.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("tile");
        }
      });
      assertEquals(1, list.length);
      assertEquals("tile-11-0-0.png", list[0]);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testOneLevelMapReduce() {
    try {
      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.set("levels", "11..11");
      params.set("mbr", "0,0,2048,2048");
      params.set("shape", "rect");
      params.setBoolean("overwrite", true);
      params.setBoolean("vflip", false);
      
      MultilevelPlot.plot(new Path[] { inFileName },
          outFileName, GeometricPlot.GeometricRasterizer.class,
          params);

      File outPath = new File(outFileName.toString());
      String[] list = outPath.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("tile");
        }
      });
      assertEquals(1, list.length);
      assertEquals("tile-11-0-0.png", list[0]);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testMultipleLevelsMapReducePyramidPartitioning() {
    try {
      int levels = 5;
      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setInt("levels", levels);
      params.set("mbr", "0,0,2048,2048");
      params.set("shape", "rect");
      params.setBoolean("overwrite", true);
      params.setBoolean("vflip", false);
      // Enforce the use of pyramid partitioning only
      params.setInt(MultilevelPlot.FlatPartitioningLevelThreshold, -1);
      
      MultilevelPlot.plot(new Path[] { inFileName },
          outFileName, GeometricPlot.GeometricRasterizer.class,
          params);

      File outPath = new File(outFileName.toString());
      List<String> list = Arrays.asList(outPath.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("tile");
        }
      }));
      assertEquals(levels, list.size());
      for (int level = 0; level < levels; level++) {
        String fileName = String.format("tile-%d-0-0.png", level);
        list.indexOf(fileName);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public void testMultilevelMapReducePyramidPartitioningWithMultipleTiles() {
    try {
      String levels = "5..5";
      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.set("levels", levels);
      params.set("mbr", "0,0,8,8");
      params.set("shape", "rect");
      params.setBoolean("overwrite", true);
      params.setBoolean("vflip", false);
      params.setInt(MultilevelPlot.FlatPartitioningLevelThreshold, 4);
      
      MultilevelPlot.plot(new Path[] { inFileName },
          outFileName, GeometricPlot.GeometricRasterizer.class,
          params);

      File outPath = new File(outFileName.toString());
      List<String> list = Arrays.asList(outPath.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("tile");
        }
      }));
      // The object spans four tiles
      assertEquals(4, list.size());
      assertTrue(list.contains("tile-5-0-0.png"));
      assertTrue(list.contains("tile-5-1-0.png"));
      assertTrue(list.contains("tile-5-0-1.png"));
      assertTrue(list.contains("tile-5-1-1.png"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
