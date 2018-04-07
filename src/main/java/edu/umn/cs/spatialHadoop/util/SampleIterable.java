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
  private FSDataInputStream in;

  /**The starting position of the file*/
  private final long start;
  /**The end offset of the file*/
  private final long end;

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
    this.in = fs.open(fsplit.getPath());
    in.seek(this.start = fsplit.getStart());
    this.end = fsplit.getStart() + fsplit.getLength();
    this.currentValue = new Text2();
    this.nextValue = new Text2();
    this.random = new Random(seed);
    this.ratio = ratio;
    prefetchNext();
  }

  public SampleIterable(FSDataInputStream in, long dataStart, long dataEnd, float ratio, long seed) throws IOException {
    this.in = in;
    in.seek(this.start = dataStart);
    this.end = dataEnd;
    this.currentValue = new Text2();
    this.nextValue = new Text2();
    this.random = new Random(seed);
    this.ratio = ratio;
    prefetchNext();
  }

  @Override
  public Iterator<Text> iterator() {
    return this;
  }

  public void prefetchNext() {
    try {
      while ((pos = in.getPos()) < end) {
        if (random.nextFloat() < ratio) {
          do {
            nextValue.clear();
            pos += readUntilEOL(in, nextValue);
          } while (nextValue.getLength() == 0 && pos < end);
          if (nextValue.getLength() == 0)
            nextValue = null;
          return;
        } else {
          skipToEOL(in);
        }
      }
      // Reached end of file
      nextValue = null;
    } catch (IOException e){
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
   * @return - number of bytes read including EOL characters
   * @throws IOException
   */
  public static int readUntilEOL(InputStream in, Text line) throws IOException {
    final byte[] bufferBytes = new byte[1024];
    int bufferLength = 0; // Length of the buffer
    do {
      if (bufferLength == bufferBytes.length) {
        // Buffer full. Copy to the output text
        line.append(bufferBytes, 0, bufferLength);
        bufferLength = 0;
      }
      if (bufferLength == 0) {
        // Read and skip any initial EOL characters
        do {
          bufferBytes[0] = (byte) in.read();
        } while (bufferBytes[0] != -1 &&
            (bufferBytes[0] == '\n' || bufferBytes[0] == '\r'));
        if (bufferBytes[0] != -1)
          bufferLength++;
      } else {
        bufferBytes[bufferLength++] = (byte) in.read();
      }
    } while (bufferLength > 0 &&
        bufferBytes[bufferLength-1] != -1 &&
        bufferBytes[bufferLength-1] != '\n' && bufferBytes[bufferLength-1] != '\r');
    if (bufferLength > 0) {
      bufferLength--;
      line.append(bufferBytes, 0, bufferLength);
    }
    return line.getLength();
  }

  /**
   * Read and discard from the input stream until an EOL is reached. EOL bytes
   * are also discarded. Returns the total number of bytes read from the input.
   * @param in
   * @throws IOException
   */
  public static int skipToEOL(InputStream in) throws IOException {
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
    return size;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
    in = null;
  }
}
