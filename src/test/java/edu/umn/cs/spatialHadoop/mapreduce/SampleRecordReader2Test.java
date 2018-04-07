package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

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
}