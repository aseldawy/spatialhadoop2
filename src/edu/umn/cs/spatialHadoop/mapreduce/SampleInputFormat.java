package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An input format that reads a sample
 * @author Ahmed Eldawy
 *
 */
public class SampleInputFormat extends FileInputFormat<NullWritable, Text> {
  private static final Log LOG = LogFactory.getLog(SampleInputFormat.class);
  
  /**A compression codec factory used to check if a file is compressed*/
  private CompressionCodecFactory compressionCodecFactory;
  
  @Override
  public RecordReader<NullWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext task) throws IOException,
      InterruptedException {
    if (split instanceof FileSplit) {
      // Check if the file is compressed
      if (compressionCodecFactory == null)
        compressionCodecFactory = new CompressionCodecFactory(task.getConfiguration());
      if (compressionCodecFactory.getCodec(((FileSplit)split).getPath()) == null)
        return new SampleRecordReaderFlat(split, task);
    }
    return new SampleRecordReaderGeneral(split, task);
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    // TODO combine splits that reside on the same machine
    return splits;
  }
}