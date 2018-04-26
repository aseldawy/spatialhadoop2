package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.io.Text2;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Random;

/**
 * Iterates over a sample of an input file
 */
public class SampleIterable implements Iterable<Text>, Iterator<Text>, Closeable {
  /**Input stream over the input*/
  private InputStream in;

  /**The starting position of the file*/
  private long start;
  /**The end offset of the file*/
  private long end;

  /**A flag that is raised when the end-of-file is reached*/
  private boolean eosReached;

  /**The current position in the file to report the progress*/
  private long pos;

  /**The sampling ratio*/
  private final float ratio;

  /**A mutable text value to iterate over the input*/
  protected Text currentValue;
  /**A mutable text that is used to prefetch next object*/
  protected Text nextValue;

  /**The random number generator associated with this iterable*/
  protected Random random;

  /**
   * Iterates over a sample of (roughly) the given ratio over a file split
   * @param fs the file system that contains the file
   * @param fsplit
   * @param ratio the average fraction of records to sample [0, 1]
   * @param seed the seed used to initialize the random number generator
   */
  public SampleIterable(FileSystem fs, FileSplit fsplit, float ratio, long seed) throws IOException {
    FSDataInputStream in = fs.open(fsplit.getPath());
    in.seek(this.start = fsplit.getStart());
    this.in = in;
    this.pos = fsplit.getStart();
    this.end = fsplit.getStart() + fsplit.getLength();
    this.currentValue = new Text2();
    this.nextValue = new Text2();
    this.random = new Random(seed);
    this.ratio = ratio;
    this.eosReached = false;
    prefetchNext();
  }

  public SampleIterable(FSDataInputStream in, long dataStart, long dataEnd, float ratio, long seed) throws IOException {
    this.in = in;
    in.seek(this.start = dataStart);
    this.pos = in.getPos();
    this.end = dataEnd;
    this.currentValue = new Text2();
    this.nextValue = new Text2();
    this.random = new Random(seed);
    this.ratio = ratio;
    this.eosReached = false;
    prefetchNext();
  }

  /**
   * Initialize from a non-bound input stream. We will keep sampling from the
   * stream until its EOF is reached.
   * @param in
   * @param ratio
   * @param seed
   * @throws IOException
   */
  public SampleIterable(InputStream in, float ratio, long seed) throws IOException {
    this.in = in;
    this.currentValue = new Text2();
    this.nextValue = new Text2();
    this.random = new Random(seed);
    // Since the stream is unbounded, we set the end to the biggest number to
    // ensure we read until the end of the stream
    this.start = 0;
    this.end = Long.MAX_VALUE;
    this.ratio = ratio;
    this.eosReached = false;
    prefetchNext();
  }

  @Override
  public Iterator<Text> iterator() {
    return this;
  }

  public void prefetchNext() {
    try {
      while (pos < end && !eosReached) {
        if (random.nextFloat() < ratio) {
          do {
            nextValue.clear();
            pos += readUntilEOL(in, nextValue);
          } while (nextValue.getLength() == 0 && !eosReached);
          if (eosReached && nextValue.getLength() == 0)
            nextValue = null;
          return;
        } else {
          // Read and discard the lines
          pos += skipToEOL(in);
        }
      }
      // Reached end of stream
      nextValue = null;
    } catch (IOException e) {
      nextValue = null;
    }
  }

  @Override
  public boolean hasNext() {
    return nextValue != null;
  }

  @Override
  public Text next() {
    // Swap currentValue <-> nextValue
    Text temp = nextValue;
    nextValue = currentValue;
    currentValue = temp;
    prefetchNext();
    return currentValue;
  }

  public float getProgress() {
    return ((float) (pos - start)) / (end - start);
  }

  public void remove() {
    throw new RuntimeException("Not implemented!");
  }

  /**
   * Read from the given stream until end-of-line is reached.
   * @param in - the input stream from where to read the line
   * @param line - the line that has been read from file not including EOL
   * @return - number of bytes read from the stream including EOL characters
   * @throws IOException
   */
  public int readUntilEOL(InputStream in, Text line) throws IOException {
    // Note: We do not check for the end of the file split because by design we
    // should go beyond the end of the split to read a line that spans two splits
    int lastByteRead;
    final byte[] bufferBytes = new byte[1024];
    int bytesRead = 0;
    int lineLength = 0; // Length of the buffer
    do {
      if (lineLength == bufferBytes.length) {
        // Buffer full. Copy to the output text
        line.append(bufferBytes, 0, lineLength);
        lineLength = 0;
      }
      if (lineLength == 0) {
        // Nothing was read yet, read and skip any initial EOL characters
        // These are EOL characters left from the previous line
        do {
          lastByteRead = in.read();
          if (lastByteRead != -1)
            bytesRead++;
          else
            eosReached = true;
        } while (!eosReached && (lastByteRead == '\n' || lastByteRead == '\r'));
        // If the last byte read was not an EOF character, use it as the first
        // character in the new line
        if (!eosReached)
          bufferBytes[lineLength++] = (byte) lastByteRead;
      } else {
        // Some bytes were read, read one more byte
        lastByteRead = in.read();
        if (lastByteRead != -1) {
          bytesRead++;
          bufferBytes[lineLength++] = (byte) lastByteRead;
        } else {
          eosReached = true;
        }
      }
    } while (!eosReached && lastByteRead != '\n' && lastByteRead != '\r');
    if (lineLength > 0) {
      // Write bufferBytes to the output without the terminating EOL character
      if (lastByteRead == '\n' || lastByteRead == '\r')
        lineLength--;
      line.append(bufferBytes, 0, lineLength);
    } else {
      line.clear();
    }
    return bytesRead;
  }

  /**
   * Read and discard from the input stream until an EOL is reached. EOL bytes
   * are also discarded. Returns the total number of bytes read from the input.
   * @param in
   * @throws IOException
   */
  public int skipToEOL(InputStream in) throws IOException {
    int size = 0;
    // Read and skip any initial EOL characters (left from previous read)
    int b;
    do {
      b = in.read();
      size++;
    } while (b != -1 && (b == '\n' || b == '\r'));

    // At this point, we read a non EOL character
    // Continue reading until an EOL is reached
    do {
      b = in.read();
      size++;
    } while (b != -1 && (b != '\n' && b != '\r'));
    if (b == -1)
      eosReached = true;
    return size;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
    in = null;
  }
}
