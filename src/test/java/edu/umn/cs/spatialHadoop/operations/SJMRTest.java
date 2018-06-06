package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;
import edu.umn.cs.spatialHadoop.indexing.STRPartitioner;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class SJMRTest extends BaseTest {

  public void testSjmr() throws IOException, InterruptedException {
    Path inFile = new Path("src/test/resources/test.rect");
    Path inFile1 = new Path(scratchPath, "file1");
    Path inFile2 = new Path(scratchPath, "file2");
    Path outFile = new Path(scratchPath, "sjmrout");

    OperationsParams params = new OperationsParams();
    FileSystem fs = inFile.getFileSystem(params);
    fs.copyToLocalFile(inFile, inFile1);
    fs.copyToLocalFile(inFile, inFile2);
    params.setClass("shape", Rectangle.class, Shape.class);
    SJMR.sjmr(new Path[]{inFile1, inFile2}, outFile, params);
    String[] results = readTextFile(outFile.toString());
    assertEquals(14, results.length);
  }

  public void testSjmrWithSTRPartitioner() throws IOException, InterruptedException {
    Path inFile = new Path("src/test/resources/test.rect");
    Path inFile1 = new Path(scratchPath, "file1");
    Path inFile2 = new Path(scratchPath, "file2");
    Path outFile = new Path(scratchPath, "sjmrout");

    OperationsParams params = new OperationsParams();
    params.setClass("partitioner", STRPartitioner.class, Partitioner.class);
    params.setFloat(SpatialSite.SAMPLE_RATIO, 1.0f);
    FileSystem fs = inFile.getFileSystem(params);
    fs.copyToLocalFile(inFile, inFile1);
    fs.copyToLocalFile(inFile, inFile2);
    params.setClass("shape", Rectangle.class, Shape.class);
    SJMR.sjmr(new Path[]{inFile1, inFile2}, outFile, params);
    String[] results = readTextFile(outFile.toString());
    assertEquals(14, results.length);
  }
}