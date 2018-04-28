package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.io.InputSubstream;
import edu.umn.cs.spatialHadoop.util.SampleIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

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
    FileSystem fs = fsplit.getPath().getFileSystem(conf);

    // Check if file is compressed
    CompressionCodec codec = getCCFactory(conf).getCodec(fsplit.getPath());
    if (codec == null) {
      // The file is not compressed, open it as a regular text file
      FSDataInputStream in = fs.open(fsplit.getPath());
      in.seek(fsplit.getStart());
      if (fsplit.getStart() != 0) {
        // Reading from a middle of a block, skip the first line which should
        // be read as part of the previous block
        skipToEOL(in);
      }

      this.sampleIterable = new SampleIterable(in, fsplit.getStart(),
          fsplit.getStart() + fsplit.getLength(), ratio, seed);
    } else {
      // A compressed file
      FSDataInputStream in = fs.open(fsplit.getPath());

      Decompressor decompressor;
      synchronized (ccFactory) {
        // CodecPool is not thread-safe
        decompressor = CodecPool.getDecompressor(codec);
      }

      if (codec instanceof SplittableCompressionCodec) {
        // A splittable codec that can read part of the input
        final SplitCompressionInputStream cIn =
            ((SplittableCompressionCodec)codec).createInputStream(
                in, decompressor, fsplit.getStart(), fsplit.getStart() + fsplit.getLength(),
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        long start = cIn.getAdjustedStart();
        long end = cIn.getAdjustedEnd();
        if (fsplit.getStart() != 0) {
          // Reading from a middle of a block, skip the first line which should
          // be read as part of the previous block
          skipToEOL(cIn);
        }
        this.sampleIterable = new SampleIterable(cIn, start, end, ratio, seed);
      } else {
        // A non-splittable codec
        in.seek(fsplit.getStart());
        InputStream ins = codec.createInputStream(new InputSubstream(in, fsplit.getLength()), decompressor);
        // We cannot skip a line when reading a non-splittable compressed file
        this.sampleIterable = new SampleIterable(ins, ratio, seed);
      }

    }
  }

  private static void skipToEOL(InputStream cIn) throws IOException {
    int b;
    do {
      b = cIn.read();
    } while (b != -1 && b != '\n' && b != '\r');
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