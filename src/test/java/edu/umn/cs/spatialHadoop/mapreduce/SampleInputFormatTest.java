package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.indexing.RRStarLocalIndex;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

public class SampleInputFormatTest extends TestCase {
  public SampleInputFormatTest(String testName) {
    super(testName);
  }

  /**A scratch area used to do all the tests which gets wiped at the end*/
  protected Path scratchPath = new Path("testindex");

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

  public void testSampleConcatenatedRTreeFile() {
    try {
      OperationsParams params = new OperationsParams();
      Path concatFile = new Path(scratchPath, "concat.rrstar");
      FileSystem fs = concatFile.getFileSystem(params);
      FSDataOutputStream out = fs.create(concatFile, true);
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);

      File[] inputFiles = new File[] {
          new File("src/test/resources/test.points"),
          new File("src/test/resources/test111.points"),
      };

      for (File file : inputFiles) {
        Path tempRtreeFile = new Path(scratchPath, "temp.rrstar");
        localIndexer.buildLocalIndex(file, tempRtreeFile, new Point());
        InputStream in = new FileInputStream(tempRtreeFile.toString());
        IOUtils.copyBytes(in, out, params, false);
        in.close();
      }
      out.close();

      // Sample all records in the file
      params.setFloat("ratio", 1.0f);
      Path fileToSample = concatFile;
      SampleInputFormat sinputFormat = new SampleInputFormat();
      Job job = Job.getInstance(params);
      SampleInputFormat.addInputPath(job, fileToSample);
      List<InputSplit> splits = sinputFormat.getSplits(job);
      int count = 0;
      for (InputSplit split : splits) {
        RecordReader<NullWritable, Text> srr =
            sinputFormat.createRecordReader(split, new TaskAttemptContextImpl(params, new TaskAttemptID()));
        while (srr.nextKeyValue())
          ++count;
        srr.close();
      }
      assertEquals(11+111, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }

  }
}