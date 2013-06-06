package edu.umn.cs.spatialHadoop.mapred;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * A base class to read shapes from files. It reads either single shapes,
 * list of shapes, or R-trees. It automatically detects the format of the
 * underlying block and parses it accordingly.
 * @author Ahmed ELdawy
 *
 */
public abstract class SpatialRecordReader<K, V> implements RecordReader<K, V> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader.class);
  
  /**Maximum number of shapes to read in one operation to return as array*/
  private int maxShapesInOneRead;
  /**Maximum size in bytes that can be read in one read*/
  private int maxBytesInOneRead;
  
  enum BlockType { HEAP, RTREE};
  
  /** First offset that is read from the input */
  protected long start;
  /** Last offset to stop at */
  protected long end;
  /** Position of the next byte to read/prase from the input */
  protected long pos;
  /** Input stream that reads from file */
  private InputStream in;
  /** Reads lines from text files */
  protected LineReader lineReader;
  /** A temporary text to read lines from lineReader */
  protected Text tempLine = new Text();
  /** Some bytes that were read from the stream but not parsed yet */
  protected byte[] buffer;

  /** File system of the file being parsed */
  private FileSystem fs;

  /** The path of the parsed file */
  private Path path;

  /** Block size for the read file. Used with RTrees */
  protected long blockSize;

  /** A cached value for the current cellInfo */
  protected Rectangle cellMbr;

  /**The type of the currently parsed block*/
  protected BlockType blockType;
  
  /**
   * Initialize from an input split
   * @param split
   * @param conf
   * @param reporter
   * @param index
   * @throws IOException
   */
  public SpatialRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    this(conf, split.getStartOffsets()[index], split.getLength(index),
        split.getPath(index));
  }
  
  /**
   * Initialize from a FileSplit
   * @param job
   * @param split
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split.getStart(), split.getLength(), split.getPath());
  }

  /**
   * Initialize from a path and range
   * @param job
   * @param s
   * @param l
   * @param p
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, long s, long l, Path p) throws IOException {
    this.start = s;
    this.end = s + l;
    this.path = p;
    this.fs = this.path.getFileSystem(job);
    this.in = fs.open(this.path);
    this.blockSize = fs.getFileStatus(this.path).getBlockSize();
    this.cellMbr = new Rectangle();
    
    LOG.info("Open a SpatialRecordReader to file: "+this.path);

    final CompressionCodec codec = new CompressionCodecFactory(job).getCodec(this.path);

    if (codec != null) {
      // Decompress the stream
      in = codec.createInputStream(in);
      // Read till the end of the stream
      end = Long.MAX_VALUE;
    } else {
      ((FSDataInputStream)in).seek(start);
    }
    this.pos = start;
    this.maxShapesInOneRead = job.getInt(SpatialSite.MaxShapesInOneRead, 1000000);
    this.maxBytesInOneRead = job.getInt(SpatialSite.MaxBytesInOneRead, 32*1024*1024);

    initializeReader();
  }
  
  /**
   * Construct from an input stream already set to the first byte
   * to read.
   * @param in
   * @param offset
   * @param endOffset
   * @throws IOException
   */
  public SpatialRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    this.in = in;
    this.start = offset;
    this.end = endOffset;
    this.pos = offset;
    this.cellMbr = new Rectangle();
    initializeReader();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    if (lineReader != null) {
      lineReader.close();
    } else if (in != null) {
      in.close();
    }
    lineReader = null;
    in = null;
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  /**
   * @throws IOException
   */
  protected boolean initializeReader() throws IOException {
    // Get the cell info for the current block
    cellMbr.invalidate(); // Initialize to invalid rectangle
    if (path != null) {
      GlobalIndex<Partition> globalIndex =
          SpatialSite.getGlobalIndex(fs, path.getParent());
      if (globalIndex != null) {
        for (Partition partition : globalIndex) {
          if (partition.filename.equals(path.getName())) {
            cellMbr.set(partition);
          }
        }
      }
    }
    
    // Read the first part of the block to determine its type
    buffer = new byte[8];
    in.read(buffer);
    if (Arrays.equals(buffer, SpatialSite.RTreeFileMarkerB)) {
      blockType = BlockType.RTREE;
      pos += 8;
      LOG.info("Block is RTree indexed at position "+pos);
      // Ignore the signature
      buffer = null;
    } else {
      LOG.info("Found a heap block at position "+pos);
      blockType = BlockType.HEAP;
      // The read buffer might contain some data that must be read
      // File is text file
      lineReader = new LineReader(in);
  
      // Skip the first line unless we are reading the first block in file
      // For globally indexed blocks, never skip the first line in the block
      boolean skipFirstLine = getPos() != 0;
      if (skipFirstLine) {
        LOG.info("Skip line");
        // Search for the first occurrence of a new line
        int eol = RTree.skipToEOL(buffer, 0);
        // If we found an end of line in the buffer, we do not need to skip
        // a line from the open stream. This happens if the EOL returned is
        // beyond the end of buffer and the buffer is not a complete line
        // by itself
        boolean skip_another_line_from_stream = eol >= buffer.length &&
            buffer[buffer.length - 1] != '\n';
        if (eol < buffer.length) {
          // Found an EOL in the buffer and there are some remaining bytes
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
          buffer = tmp;
          // Advance current position to skip the first partial line
          this.pos += eol;
        } else {
          // Did not find an EOL in the buffer or found it at the very end
          pos += buffer.length;
          // Buffer does not contain any useful data
          buffer = null;
        }
        
        if (skip_another_line_from_stream) {
          // Didn't find an EOL in the buffer, need to skip it from the stream
          pos += lineReader.readLine(tempLine, 0, (int)(end - pos));
        }
      }
      LOG.info("After line skip: "+(buffer == null? 0 : buffer.length)+" bytes in buffer and pos="+pos);
    }
    
    
    return true;
  }

  /**
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value
   * @return
   * @throws IOException
   */
  protected boolean nextLine(Text value) throws IOException {
    if (blockType == BlockType.RTREE && pos == 8) {
      // File is positioned at the RTree header
      // Skip the header and go to first data object in file
      pos += RTree.skipHeader(in);
      LOG.info("Skipped R-tree to position: "+pos);
      // Reinitialize record reader at the new position
      lineReader = new LineReader(in);
    }
    while (getPos() <= end) {
      value.clear();
      int b = 0;
      if (buffer != null) {
        // Read the first line encountered in buffer
        int eol = RTree.skipToEOL(buffer, 0);
        b += eol;
        value.append(buffer, 0, eol);
        if (eol < buffer.length) {
          // There are still some bytes remaining in buffer
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
        } else {
          buffer = null;
        }
        // Check if a complete line has been read from the buffer
        byte last_byte = value.getBytes()[value.getLength()-1];
        if (last_byte == '\n' || last_byte == '\r')
          return true;
      }
      
      // Read the first line from stream
      Text temp = new Text();
      b += lineReader.readLine(temp);
      if (b == 0) {
        // Indicates an end of stream
        return false;
      }
      pos += b;
      
      // Append the part read from stream to the part extracted from buffer
      value.append(temp.getBytes(), 0, temp.getLength());
      
      if (value.getLength() > 1) {
        // Read a non-empty line. Note that end-of-line character is included
        return true;
      }
    }
    // Reached end of file
    return false;
  }

  /**
   * Reads next shape from input and returns true. If no more shapes are left
   * in the split, a false is returned.
   * @param s
   * @return
   * @throws IOException 
   */
  protected boolean nextShape(Shape s) throws IOException {
    if (!nextLine(tempLine))
      return false;
    s.fromText(tempLine);
    return true;
  }
  
  /**
   * Reads all shapes left in the current block in one shot.
   * @param shapes
   * @return
   * @throws IOException
   */
  protected boolean nextShapes(ArrayWritable shapes) throws IOException {
    // Prepare a vector that will hold all objects in this 
    Vector<Shape> vshapes = new Vector<Shape>();
    try {
      Shape stockObject = (Shape) shapes.getValueClass().newInstance();
      // Reached the end of this split
      if (getPos() >= end)
        return false;
      
      long initialReadPos = getPos();
      long readBytes = 0;
      
      // Read all shapes in this block
      while (vshapes.size() < maxShapesInOneRead &&
          readBytes < maxBytesInOneRead && nextShape(stockObject)) {
        vshapes.add(stockObject.clone());
        readBytes = getPos() - initialReadPos;
      }

      // Store them in the return value
      shapes.set(vshapes.toArray(new Shape[vshapes.size()]));
      
      return vshapes.size() > 0;
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    } catch (OutOfMemoryError e) {
      LOG.error("Error reading shapes. Stopped with "+vshapes.size()+" shapes");
      throw e;
    }
    return false;
  }
  
  /**
   * Reads the next RTree from file.
   * @param rtree
   * @return
   * @throws IOException
   */
  protected boolean nextRTree(RTree<? extends Shape> rtree) throws IOException {
    if (blockType == BlockType.RTREE) {
      if (getPos() != 8)
        return false;
      // Signature was already read in initialization.
      buffer = null;
      DataInput dataIn = in instanceof DataInput?
          (DataInput) in : new DataInputStream(in);
      rtree.readFields(dataIn);
      pos++;
      return true;
    } else {
      throw new RuntimeException("Not implemented");
    }
  }
}
