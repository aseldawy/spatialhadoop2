package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.io.InputSubstream;
import edu.umn.cs.spatialHadoop.util.SampleIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Random;

/**
 * A record reader to read sample of a compressed file
 * @author Ahmed Eldawy
 *
 */
public class SampleRecordReaderTextFile extends RecordReader<NullWritable, Text> {

  /**Split to read from*/
  protected FileSplit fsplit;

  /**Sampling ratio*/
  protected float ratio;

  /**Configuration used to obtain FileSystem of file splits*/
  protected Configuration conf;

  /**An iterable that iterates over the sample records*/
  private SampleIterable sampleIterable;

  /**The seed of the random number generator*/
  private long seed;

  /**Current value*/
  protected Text value;

  /**Compression codec factory*/
  private static CompressionCodecFactory ccFactory;

  public SampleRecordReaderTextFile() {
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext task)
      throws IOException {
    this.fsplit = (FileSplit) split;

    this.ratio = task.getConfiguration().getFloat("ratio", 0.01f);
    this.conf = task.getConfiguration();
    this.seed = conf.getLong("seed", System.currentTimeMillis());

    // Check if file is compressed
    CompressionCodec codec = getCCFactory(conf).getCodec(fsplit.getPath());
    if (codec == null) {
      // The file is not compressed, open it as a regular text file
      this.sampleIterable = new SampleIterable(fsplit.getPath().getFileSystem(conf),
          fsplit, ratio, seed);
    } else {
      Decompressor decompressor;
      synchronized (ccFactory) {
        // CodecPool is not thread-safe
        decompressor = CodecPool.getDecompressor(codec);
      }
      FileSystem fs = fsplit.getPath().getFileSystem(conf);
      FSDataInputStream in = fs.open(fsplit.getPath());
      in.seek(fsplit.getStart());
      InputStream ins = codec.createInputStream(new InputSubstream(in, fsplit.getLength()), decompressor);
      this.sampleIterable = new SampleIterable(ins, ratio, seed);
    }
  }

  private static CompressionCodecFactory getCCFactory(Configuration conf) {
    if (ccFactory != null)
      return ccFactory;
    synchronized (CompressionCodecFactory.class) {
      if (ccFactory == null)
        ccFactory = new CompressionCodecFactory(conf);
      return ccFactory;
    }
  }

  @Override
  public boolean nextKeyValue() {
    if (sampleIterable.hasNext()) {
      value = sampleIterable.next();
      return true;
    }
    value = null;
    return false;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return sampleIterable.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (sampleIterable != null)
      sampleIterable.close();
  }
  
}