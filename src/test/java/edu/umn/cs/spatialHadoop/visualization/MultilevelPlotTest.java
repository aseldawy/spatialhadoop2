package edu.umn.cs.spatialHadoop.visualization;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;

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

    public void testMultilevelPlotLocal() {
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

    public void testMultilevelPlotMapReduce() {
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
}
