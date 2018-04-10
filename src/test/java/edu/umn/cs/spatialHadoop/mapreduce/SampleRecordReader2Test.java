package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.indexing.RRStarLocalIndex;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class SampleRecordReader2Test extends TestCase {

  public SampleRecordReader2Test(String testName) {
    super(testName);
  }

  public void testFullSampleTextFile() {
    Path fileToSample = new Path("src/test/resources/test.points");
    OperationsParams params = new OperationsParams();
    try {
      FileSystem fs = fileToSample.getFileSystem(params);
      FileSplit fsplit = new FileSplit(fileToSample, 0, fs.getFileStatus(fileToSample).getLen(), null);
      SampleRecordReaderTextFile srr = new SampleRecordReaderTextFile();
      params.setFloat("ratio", 1.0f);
      srr.initialize(fsplit, new TaskAttemptContextImpl(params, new TaskAttemptID()));

      int count = 0;
      while (srr.nextKeyValue())
        ++count;
      assertEquals(11, count);
      assertEquals(1.0f, srr.getProgress());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }

  public void testFullSampleRTreeFile() {
    try {
      // Index the first file
      OperationsParams params = new OperationsParams();
      Path concatFile = new Path("concat.rstar");
      FileSystem fs = concatFile.getFileSystem(params);
      FSDataOutputStream out = fs.create(concatFile, true);
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);

      File[] inputFiles = new File[]{
          new File("src/test/resources/test.points"),
          new File("src/test/resources/test111.points"),
      };

      for (File file : inputFiles) {
        String tempRtreeFile = "temp.rstar";
        localIndexer.buildLocalIndex(file, new Path(tempRtreeFile), new Point());
        InputStream in = new FileInputStream(tempRtreeFile);
        IOUtils.copyBytes(in, out, params, false);
        in.close();
      }
      out.close();

      Path fileToSample = concatFile;

      FileSplit fsplit = new FileSplit(fileToSample, 0, fs.getFileStatus(fileToSample).getLen(), null);
      SampleRecordReaderLocalIndexFile srr = new SampleRecordReaderLocalIndexFile(localIndexer);
      params.setFloat("ratio", 1.0f);
      srr.initialize(fsplit, new TaskAttemptContextImpl(params, new TaskAttemptID()));

      int count = 0;
      while (srr.nextKeyValue())
        ++count;
      assertEquals(11+111, count);
      assertEquals(1.0f, srr.getProgress());

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in test", e);
    }
  }
}