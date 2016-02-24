package edu.umn.cs.spatialHadoop.visualization;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.OperationsParams;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class MultilevelPlotTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
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

    public void testOneLevelLocal() {
        try {
            String inFileName = "test.rect";
            String outFileName = "test_pyramid";
            PrintWriter inTest = new PrintWriter(inFileName);
            inTest.println("0,0,0.5,0.5");
            inTest.close();

            OperationsParams params = new OperationsParams();
            params.setBoolean("local", true);
            params.set("levels", "11..11");
            params.set("mbr", "0,0,2048,2048");
            params.set("shape", "rect");
            params.setBoolean("overwrite", true);
            params.setBoolean("vflip", false);
            FileUtils.deleteDirectory(new File(outFileName));
            MultilevelPlot.plot(new Path[] { new Path(inFileName) }, new Path(outFileName),
                    GeometricPlot.GeometricRasterizer.class, params);

            File outPath = new File(outFileName);
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
            String inFileName = "test.rect";
            String outFileName = "test_pyramid";
            PrintWriter inTest = new PrintWriter(inFileName);
            inTest.println("0,0,0.5,0.5");
            inTest.close();

            OperationsParams params = new OperationsParams();
            params.setBoolean("local", false);
            params.set("levels", "11..11");
            params.set("mbr", "0,0,2048,2048");
            params.set("shape", "rect");
            params.setBoolean("overwrite", true);
            params.setBoolean("vflip", false);
            FileUtils.deleteDirectory(new File(outFileName));
            MultilevelPlot.plot(new Path[] { new Path(inFileName) }, new Path(outFileName),
                    GeometricPlot.GeometricRasterizer.class, params);

            File outPath = new File(outFileName + "/pyramid");
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
            String inFileName = "test.rect";
            String outFileName = "test_pyramid";
            PrintWriter inTest = new PrintWriter(inFileName);
            inTest.println("0,0,0.5,0.5");
            inTest.close();
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
            FileUtils.deleteDirectory(new File(outFileName));
            MultilevelPlot.plot(new Path[] { new Path(inFileName) }, new Path(outFileName),
                    GeometricPlot.GeometricRasterizer.class, params);

            File outPath = new File(outFileName + "/pyramid");
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
}
