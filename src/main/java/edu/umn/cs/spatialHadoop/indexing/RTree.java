/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.QuickSort;

import com.vividsolutions.jts.geom.TopologyException;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.MemoryInputStream;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * A disk-based R-tree that can be loaded using a bulk loading method and
 * never changed afterwards. It works with any shape given in the generic
 * parameter. To load the tree, use the {@link #bulkLoadWrite(byte[], int, int, int, DataOutput, Shape, boolean)}
 * method. To restore the tree from disk, use the {@link #readFields(DataInput)}
 * methods. To do queries against the tree, use the {@link #search(Shape, ResultCollector)},
 *  {@link #knn(double, double, int, ResultCollector2)} or
 *  {@link #spatialJoin(RTree, RTree, ResultCollector2, Reporter)}
 * @author Ahmed Eldawy
 *
 * @param <T>
 */
public class RTree<T extends Shape> implements Writable, Iterable<T>, Closeable {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(RTree.class);
  
  /**Size of tree header on disk. Height + Degree + Number of records*/
  public static final int TreeHeaderSize = 4 + 4 + 4;

  /**Size of a node. Offset of first child + dimensions (x, y, width, height)*/
  public static final int NodeSize = 4 + 8 * 4;

  /** An instance of T that can be used to deserialize objects from disk */
  T stockObject;
  
  /**Height of the tree (number of levels)*/
  private int height;
  
  /**Degree of internal nodes in the tree*/
  private int degree;
  
  /**Total number of nodes in the tree*/
  private int nodeCount;
  
  /**Number of leaf nodes*/
  private int leafNodeCount;
  
  /**Number of non-leaf nodes*/
  private int nonLeafNodeCount;
  
  /**Number of elements in the tree*/
  private int elementCount;
  
  /**Input stream to tree data*/
  private FSDataInputStream data;

  /**The start offset of the tree in the data stream*/
  private long treeStartOffset;
  
  /**Total tree size (header + structure + data) used to read the data in
   * the last leaf node correctly*/
  private int treeSize;

  /**A cached copy of all nodes (MBRs) in memory indexed by node ID*/
  private Rectangle[] nodes;

  /**A cached copy of data offset for each node.*/
  private int[] dataOffset;

  public RTree() {
  }
  
  /**
   * Builds the RTree given a serialized list of elements. It uses the given
   * stockObject to deserialize these elements using
   * {@link TextSerializable#fromText(Text)} and build the tree. Also writes the
   * created tree to the disk directly.
   * 
   * @param element_bytes
   *          - serialization of all elements separated by new lines
   * @param offset
   *          - offset of the first byte to use in elements_bytes
   * @param len
   *          - number of bytes to use in elements_bytes
   * @param degree
   *          - Degree of the R-tree to build in terms of number of children per
   *          node
   * @param dataOut
   *          - output stream to write the result to.
   * @param fast_sort
   *          - setting this to <code>true</code> allows the method to run
   *          faster by materializing the offset of each element in the list
   *          which speeds up the comparison. However, this requires an
   *          additional 16 bytes per element. So, for each 1M elements, the
   *          method will require an additional 16 M bytes (approximately).
   */
  public static void bulkLoadWrite(final byte[] element_bytes,
      final int offset, final int len, final int degree, DataOutput dataOut,
      final Shape stockObject, final boolean fast_sort) {
    try {

      int elementCount = 0;
      // Count number of elements in the given text
      int i_start = offset;
      final Text line = new Text();
      while (i_start < offset + len) {
        int i_end = skipToEOL(element_bytes, i_start);
        // Extract the line without end of line character
        line.set(element_bytes, i_start, i_end - i_start - 1);
        stockObject.fromText(line);
        elementCount++;
        i_start = i_end;
      }
      LOG.info("Bulk loading an RTree with "+elementCount+" elements");
      
      // It turns out the findBestDegree returns the best degree when the whole
      // tree is loaded to memory when processed. However, as current algorithms
      // process the tree while it's on disk, a higher degree should be selected
      // such that a node fits one file block (assumed to be 4K).
      //final int degree = findBestDegree(bytesAvailable, elementCount);
      
      int height = Math.max(1, 
          (int) Math.ceil(Math.log(elementCount)/Math.log(degree)));
      int leafNodeCount = (int) Math.pow(degree, height - 1);
      if (elementCount <  2 * leafNodeCount && height > 1) {
        height--;
        leafNodeCount = (int) Math.pow(degree, height - 1);
      }
      int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
      int nonLeafNodeCount = nodeCount - leafNodeCount;

      // Keep track of the offset of each element in the text
      final int[] offsets = new int[elementCount];
      final double[] xs = fast_sort? new double[elementCount] : null;
      final double[] ys = fast_sort? new double[elementCount] : null;
      
      i_start = offset;
      line.clear();
      for (int i = 0; i < elementCount; i++) {
        offsets[i] = i_start;
        int i_end = skipToEOL(element_bytes, i_start);
        if (xs != null) {
          // Extract the line with end of line character
          line.set(element_bytes, i_start, i_end - i_start - 1);
          stockObject.fromText(line);
          // Sample center of the shape
          xs[i] = (stockObject.getMBR().x1 + stockObject.getMBR().x2) / 2;
          ys[i] = (stockObject.getMBR().y1 + stockObject.getMBR().y2) / 2;
        }
        i_start = i_end;
      }

      /**A struct to store information about a split*/
      class SplitStruct extends Rectangle {
        /**Start and end index for this split*/
        int index1, index2;
        /**Direction of this split*/
        byte direction;
        /**Index of first element on disk*/
        int offsetOfFirstElement;
        
        static final byte DIRECTION_X = 0;
        static final byte DIRECTION_Y = 1;
        
        SplitStruct(int index1, int index2, byte direction) {
          this.index1 = index1;
          this.index2 = index2;
          this.direction = direction;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
          out.writeInt(offsetOfFirstElement);
          super.write(out);
        }

        void partition(Queue<SplitStruct> toBePartitioned) {
          IndexedSortable sortableX;
          IndexedSortable sortableY;

          if (fast_sort) {
            // Use materialized xs[] and ys[] to do the comparisons
            sortableX = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap xs
                double tempx = xs[i];
                xs[i] = xs[j];
                xs[j] = tempx;
                // Swap ys
                double tempY = ys[i];
                ys[i] = ys[j];
                ys[j] = tempY;
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                if (xs[i] < xs[j])
                  return -1;
                if (xs[i] > xs[j])
                  return 1;
                return 0;
              }
            };
            
            sortableY = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap xs
                double tempx = xs[i];
                xs[i] = xs[j];
                xs[j] = tempx;
                // Swap ys
                double tempY = ys[i];
                ys[i] = ys[j];
                ys[j] = tempY;
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                if (ys[i] < ys[j])
                  return -1;
                if (ys[i] > ys[j])
                  return 1;
                return 0;
              }
            };
          } else {
            // No materialized xs and ys. Always deserialize objects to compare
            sortableX = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                // Get end of line
                int eol = skipToEOL(element_bytes, offsets[i]);
                line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
                stockObject.fromText(line);
                double xi = (stockObject.getMBR().x1 + stockObject.getMBR().x2) / 2;

                eol = skipToEOL(element_bytes, offsets[j]);
                line.set(element_bytes, offsets[j], eol - offsets[j] - 1);
                stockObject.fromText(line);
                double xj = (stockObject.getMBR().x1 + stockObject.getMBR().x2) / 2;
                if (xi < xj)
                  return -1;
                if (xi > xj)
                  return 1;
                return 0;
              }
            };
            
            sortableY = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                int eol = skipToEOL(element_bytes, offsets[i]);
                line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
                stockObject.fromText(line);
                double yi = (stockObject.getMBR().y1 + stockObject.getMBR().y2) / 2;

                eol = skipToEOL(element_bytes, offsets[j]);
                line.set(element_bytes, offsets[j], eol - offsets[j] - 1);
                stockObject.fromText(line);
                double yj = (stockObject.getMBR().y1 + stockObject.getMBR().y2) / 2;
                if (yi < yj)
                  return -1;
                if (yi > yj)
                  return 1;
                return 0;
              }
            };
          }

          final IndexedSorter sorter = new QuickSort();
          
          final IndexedSortable[] sortables = new IndexedSortable[2];
          sortables[SplitStruct.DIRECTION_X] = sortableX;
          sortables[SplitStruct.DIRECTION_Y] = sortableY;
          
          sorter.sort(sortables[direction], index1, index2);

          // Partition into maxEntries partitions (equally) and
          // create a SplitStruct for each partition
          int i1 = index1;
          for (int iSplit = 0; iSplit < degree; iSplit++) {
            int i2 = index1 + (index2 - index1) * (iSplit + 1) / degree;
            SplitStruct newSplit = new SplitStruct(i1, i2, (byte)(1 - direction));
            toBePartitioned.add(newSplit);
            i1 = i2;
          }
        }
      }
      
      // All nodes stored in level-order traversal
      Vector<SplitStruct> nodes = new Vector<SplitStruct>();
      final Queue<SplitStruct> toBePartitioned = new LinkedList<SplitStruct>();
      toBePartitioned.add(new SplitStruct(0, elementCount, SplitStruct.DIRECTION_X));
      
      while (!toBePartitioned.isEmpty()) {
        SplitStruct split = toBePartitioned.poll();
        if (nodes.size() < nonLeafNodeCount) {
          // This is a non-leaf
          split.partition(toBePartitioned);
        }
        nodes.add(split);
      }
      
      if (nodes.size() != nodeCount) {
        throw new RuntimeException("Expected node count: "+nodeCount+". Real node count: "+nodes.size());
      }
      
      // Now we have our data sorted in the required order. Start building
      // the tree.
      // Store the offset of each leaf node in the tree
      FSDataOutputStream fakeOut = null;
      try {
        fakeOut = new FSDataOutputStream(new java.io.OutputStream() {
          // Null output stream
          @Override
          public void write(int b) throws IOException {
            // Do nothing
          }
          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            // Do nothing
          }
          @Override
          public void write(byte[] b) throws IOException {
            // Do nothing
          }
        }, null, TreeHeaderSize + nodes.size() * NodeSize);
        for (int i_leaf = nonLeafNodeCount, i=0; i_leaf < nodes.size(); i_leaf++) {
          nodes.elementAt(i_leaf).offsetOfFirstElement = (int)fakeOut.getPos();
          if (i != nodes.elementAt(i_leaf).index1) throw new RuntimeException();
          double x1, y1, x2, y2;
          
          // Initialize MBR to first object
          int eol = skipToEOL(element_bytes, offsets[i]);
          fakeOut.write(element_bytes, offsets[i],
              eol - offsets[i]);
          line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
          stockObject.fromText(line);
          Rectangle mbr = stockObject.getMBR();
          x1 = mbr.x1;
          y1 = mbr.y1;
          x2 = mbr.x2;
          y2 = mbr.y2;
          i++;
          
          while (i < nodes.elementAt(i_leaf).index2) {
            eol = skipToEOL(element_bytes, offsets[i]);
            fakeOut.write(element_bytes, offsets[i],
                eol - offsets[i]);
            line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
            stockObject.fromText(line);
            mbr = stockObject.getMBR();
            if (mbr.x1 < x1) x1 = mbr.x1;
            if (mbr.y1 < y1) y1 = mbr.y1;
            if (mbr.x2 > x2) x2 = mbr.x2;
            if (mbr.y2 > y2) y2 = mbr.y2;
            i++;
          }
          nodes.elementAt(i_leaf).set(x1, y1, x2, y2);
        }
        
      } finally {
        if (fakeOut != null)
          fakeOut.close();
      }
      
      // Calculate MBR and offsetOfFirstElement for non-leaves
      for (int i_node = nonLeafNodeCount-1; i_node >= 0; i_node--) {
        int i_first_child = i_node * degree + 1;
        nodes.elementAt(i_node).offsetOfFirstElement =
            nodes.elementAt(i_first_child).offsetOfFirstElement;
        int i_child = 0;
        Rectangle mbr;
        mbr = nodes.elementAt(i_first_child + i_child);
        double x1 = mbr.x1;
        double y1 = mbr.y1;
        double x2 = mbr.x2;
        double y2 = mbr.y2;
        i_child++;
        
        while (i_child < degree) {
          mbr = nodes.elementAt(i_first_child + i_child);
          if (mbr.x1 < x1) x1 = mbr.x1;
          if (mbr.y1 < y1) y1 = mbr.y1;
          if (mbr.x2 > x2) x2 = mbr.x2;
          if (mbr.y2 > y2) y2 = mbr.y2;
          i_child++;
        }
        nodes.elementAt(i_node).set(x1, y1, x2, y2);
      }
      
      // Start writing the tree
      // write tree header (including size)
      // Total tree size. (== Total bytes written - 8 bytes for the size itself)
      dataOut.writeInt(TreeHeaderSize + NodeSize * nodeCount + len);
      // Tree height
      dataOut.writeInt(height);
      // Degree
      dataOut.writeInt(degree);
      dataOut.writeInt(elementCount);
      
      // write nodes
      for (SplitStruct node : nodes) {
        node.write(dataOut);
      }
      // write elements
      for (int element_i = 0; element_i < elementCount; element_i++) {
        int eol = skipToEOL(element_bytes, offsets[element_i]);
        dataOut.write(element_bytes, offsets[element_i],
            eol - offsets[element_i]);
      }
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    throw new RuntimeException("write is no longer supported. " +
    		"Please use bulkLoadWrite to write the RTree.");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // Read the whole tree structure and keep it in memory. Leave data on disk
    // Tree size (Header + structure + data)
    treeSize = in.readInt();
    
    if (in instanceof Seekable)
      this.treeStartOffset = ((Seekable) in).getPos();
    if (treeSize == 0) {
      height = elementCount = 0;
      return;
    } 
    
    // Read only the tree structure in memory while actual records remain on
    // disk and loaded when necessary
    height = in.readInt();
    if (height == 0)
      return;
    degree = in.readInt();
    elementCount = in.readInt();
    
    // Keep only tree structure in memory
    nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
    this.nodes = new Rectangle[nodeCount];
    this.dataOffset = new int[nodeCount + 1];
    
    for (int node_id = 0; node_id < nodeCount; node_id++) {
      this.dataOffset[node_id] = in.readInt();
      this.nodes[node_id] = new Rectangle();
      this.nodes[node_id].readFields(in);
    }
    this.dataOffset[nodeCount] = treeSize;

    if (in instanceof FSDataInputStream) {
      // A random input stream, can keep the data on disk
      this.data = (FSDataInputStream) in;
    } else {
      // A sequential input stream, need to read all data now
      int treeDataSize = this.dataOffset[nodeCount] - this.dataOffset[0];
      // Adjust the offset of data to be zero
      this.treeStartOffset = -this.dataOffset[0];
      byte[] treeData = new byte[treeDataSize];
      in.readFully(treeData, 0, treeDataSize);
      this.data = new FSDataInputStream(new MemoryInputStream(treeData));
    }
    leafNodeCount = (int) Math.pow(degree, height - 1);
    nonLeafNodeCount = nodeCount - leafNodeCount;
  }
  
  /**
   * Reads and skips the header of the tree returning the total number of
   * bytes skipped from the stream. This is used as a preparatory function to
   * read all elements in the tree without the index part.
   * @param in
   * @return - Total number of bytes read and skipped
   * @throws IOException
   */
  public static int skipHeader(InputStream in) throws IOException {
    DataInput dataIn = in instanceof DataInput ? (DataInput) in
        : new DataInputStream(in);
    int skippedBytes = 0;
    /*int treeSize = */dataIn.readInt(); skippedBytes += 4;
    int height = dataIn.readInt(); skippedBytes += 4;
    if (height == 0) {
      // Empty tree. No results
      return skippedBytes;
    }
    int degree = dataIn.readInt(); skippedBytes += 4;
    int nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
    /*int elementCount = */dataIn.readInt(); skippedBytes += 4;
    // Skip all nodes
    skippedBytes += dataIn.skipBytes(nodeCount * NodeSize);
    return skippedBytes;
  }
  
  /**
   * Returns the total size of the header (including the index) in bytes.
   * Assume that the input is aligned to the start offset of the tree (header).
   * Note that the part of the header is consumed from the given input to be
   * able to determine header size.
   * @param in
   * @return
   * @throws IOException
   */
  public static int getHeaderSize(DataInput in) throws IOException {
    int header_size = 0;
    /*int treeSize = */in.readInt(); header_size += 4;
    int height = in.readInt(); header_size += 4;
    if (height == 0) {
      // Empty tree. No results
      return header_size;
    }
    int degree = in.readInt(); header_size += 4;
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    /*int elementCount = */in.readInt(); header_size += 4;
    // Add the size of all nodes
    header_size += nodeCount * NodeSize;
    return header_size;
  }
  
  public long getEndOffset() {
    return treeStartOffset + treeSize;
  }

  /**
   * Returns total number of elements
   * @return
   */
  public int getElementCount() {
    return elementCount;
  }
  
  /**
   * Returns the MBR of the root
   * @return
   */
  public Rectangle getMBR() {
    return nodes[0];
  }
  
  /**
   * Reads and returns the element with the given index
   * @param i
   * @return
   */
  public T readElement(int i) {
    Iterator<T> iter = iterator();
    while (i-- > 0 && iter.hasNext()) {
      iter.next();
    }
    return iter.next();
  }

  public void setStockObject(T stockObject) {
    this.stockObject = stockObject;
  }
  
  /**
   * Create rectangles that together pack all points in sample such that
   * each rectangle contains roughly the same number of points. In other words
   * it tries to balance number of points in each rectangle.
   * Works similar to the logic of bulkLoad but does only one level of
   * rectangles.
   * @param gridInfo Used as a hint for number of rectangles per row or column
   * @param sample
   * @return
   */
  public static Rectangle[] packInRectangles(GridInfo gridInfo, final Point[] sample) {
    Rectangle[] rectangles = new Rectangle[gridInfo.columns * gridInfo.rows];
    int iRectangle = 0;
    // Sort in x direction
    final IndexedSortable sortableX = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Point temp = sample[i];
        sample[i] = sample[j];
        sample[j] = temp;
      }

      @Override
      public int compare(int i, int j) {
        if (sample[i].x < sample[j].x)
          return -1;
        if (sample[i].x > sample[j].x)
          return 1;
        return 0;
      }
    };

    // Sort in y direction
    final IndexedSortable sortableY = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Point temp = sample[i];
        sample[i] = sample[j];
        sample[j] = temp;
      }

      @Override
      public int compare(int i, int j) {
        if (sample[i].y < sample[j].y)
          return -1;
        if (sample[i].y > sample[j].y)
          return 1;
        return 0;
      }
    };

    final QuickSort quickSort = new QuickSort();
    
    quickSort.sort(sortableX, 0, sample.length);

    int xindex1 = 0;
    double x1 = gridInfo.x1;
    for (int col = 0; col < gridInfo.columns; col++) {
      int xindex2 = sample.length * (col + 1) / gridInfo.columns;
      
      // Determine extents for all rectangles in this column
      double x2 = col == gridInfo.columns - 1 ? 
          gridInfo.x2 : sample[xindex2-1].x;
      
      // Sort all points in this column according to its y-coordinate
      quickSort.sort(sortableY, xindex1, xindex2);
      
      // Create rectangles in this column
      double y1 = gridInfo.y1;
      for (int row = 0; row < gridInfo.rows; row++) {
        int yindex2 = xindex1 + (xindex2 - xindex1) * (row + 1) / gridInfo.rows;
        double y2 = row == gridInfo.rows - 1 ? gridInfo.y2 : sample[yindex2 - 1].y;
        
        rectangles[iRectangle++] = new Rectangle(x1, y1, x2, y2);
        y1 = y2;
      }
      
      xindex1 = xindex2;
      x1 = x2;
    }
    return rectangles;
  }
  
  /**
   * An iterator that goes over all elements in the tree in no particular order
   * @author eldawy
   *
   */
  class RTreeIterator implements Iterator<T> {

    /**Current offset in the data stream*/
    int offset;
    
    /**Temporary text that holds one line to deserialize objects*/
    Text line;
    
    /**A stock object to read from stream*/
    T _stockObject;
    
    /**A reader to read lines from the tree*/
    LineReader reader;
    
    RTreeIterator() throws IOException {
      offset = TreeHeaderSize + NodeSize * RTree.this.nodeCount;
      _stockObject = (T) RTree.this.stockObject.clone();
      line = new Text();
      RTree.this.data.seek(offset + RTree.this.treeStartOffset);
      reader = new LineReader(RTree.this.data);
    }

    @Override
    public boolean hasNext() {
      return offset < RTree.this.treeSize;
    }

    @Override
    public T next() {
      if (!hasNext())
        return null;
      try {
        offset += reader.readLine(line);
        _stockObject.fromText(line);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
      return _stockObject;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }
  
  /**
   * Skip bytes until the end of line
   * @param bytes
   * @param startOffset
   * @return
   */
  public static int skipToEOL(byte[] bytes, int startOffset) {
    int eol = startOffset;
    while (eol < bytes.length && (bytes[eol] != '\n' && bytes[eol] != '\r'))
      eol++;
    while (eol < bytes.length && (bytes[eol] == '\n' || bytes[eol] == '\r'))
      eol++;
    return eol;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      return new RTreeIterator();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Given a block size, record size and a required tree degree, this function
   * calculates the maximum number of records that can be stored in this
   * block taking into consideration the overhead needed by node structure.
   * @param blockSize
   * @param degree
   * @param recordSize
   * @return
   */
  public static int getBlockCapacity(long blockSize, int degree, int recordSize) {
    double a = (double)NodeSize / (degree - 1);
    double ratio = (blockSize + a) / (recordSize + a);
    double break_even_height = Math.log(ratio) / Math.log(degree);
    double h_min = Math.floor(break_even_height);
    double capacity1 = Math.floor(Math.pow(degree, h_min));
    double structure_size = 4 + TreeHeaderSize + a * (capacity1 * degree - 1);
    double capacity2 = Math.floor((blockSize - structure_size) / recordSize);
    return Math.max((int)capacity1, (int)capacity2);
  }

  /**
   * Searches the RTree starting from the given start position. This is either
   * a node number or offset of an element. If it's a node number, it performs
   * the search in the subtree rooted at this node. If it's an offset number,
   * it searches only the object found there.
   * It is assumed that the openQuery() has been called before this function
   * and that endQuery() will be called afterwards.
   * @param query_shape
   * @param output
   * @param start where to start searching
   * @param end where to end searching. Only used when start is an offset of
   *   an object.
   * @return
   * @throws IOException
   */
  protected int search(Shape query_shape, ResultCollector<T> output, int start,
      int end)
      throws IOException {
    Rectangle query_mbr = query_shape.getMBR();
    int resultSize = 0;
    // Special case for an empty tree
    if (height == 0)
      return 0;

    Stack<Integer> toBeSearched = new Stack<Integer>();
    // Start from the given node
    toBeSearched.push(start);
    if (start >= nodeCount) {
      toBeSearched.push(end);
    }

    // Holds one data line from tree data
    Text line = new Text2();
    
    while (!toBeSearched.isEmpty()) {
      int searchNumber = toBeSearched.pop();

      if (searchNumber < nodeCount) {
        // Searching a node
        int nodeID = searchNumber;
        if (query_mbr.isIntersected(nodes[nodeID])) {
          boolean is_leaf = nodeID >= nonLeafNodeCount;
          if (is_leaf) {
            // Check all objects under this node
            int start_offset = this.dataOffset[nodeID];
            int end_offset = this.dataOffset[nodeID + 1];
            toBeSearched.add(start_offset);
            toBeSearched.add(end_offset);
          } else {
            // Add all child nodes
            for (int iChild = 0; iChild < this.degree; iChild++) {
              toBeSearched.add(nodeID * this.degree + iChild + 1);
            }
          }
        }
      } else {
        // searchNumber is the end offset of data search. Start offset is next
        // in stack
        int end_offset = searchNumber;
        int start_offset = toBeSearched.pop();
        // All data offsets are relative to tree start (typically 4)
        this.data.seek(start_offset + this.treeStartOffset);
        // Should not close the line reader because we do not want to close
        // the underlying data stream now. In case future searches are done
        @SuppressWarnings("resource")
        LineReader lineReader = new LineReader(data);
        while (start_offset < end_offset) {
          start_offset += lineReader.readLine(line);
          stockObject.fromText(line);
          if (stockObject.isIntersected(query_shape)) {
            resultSize++;
            if (output != null)
              output.collect(stockObject);
          }
        }
      }
    }
    return resultSize;
  }
  
  /**
   * An iterator used to return all search results
   * @author Ahmed Eldawy
   */
  public class SearchIterator implements Iterable<T>, Iterator<T> {

    /**The last object that has returned by next*/
    private T resultShape;
    
    /**The next object that will be returned on the following next() call*/
    private T nextResultShape;

    /**MBR of the query shape for fast comparison with nodes*/
    private Rectangle queryMBR;
    
    /**Shape to search*/
    private Shape queryShape;
    
    /**Nodes or parts of the file to be searched*/
    private Stack<Integer> toBeSearched = new Stack<Integer>();
    
    /**Used to deserialize node information*/
    private Rectangle nodeMBR = new Rectangle();
    
    /**Used to deserialize record information*/
    private Text line = new Text2();
    
    /**If searching within node, these are the offsets of records in it*/
    private int firstOffset, lastOffset;
    
    /**If searching within a node, lineReader points to result items*/
    LineReader lineReader;


    public SearchIterator(Shape queryShape) {
      this.queryShape = queryShape;
      this.queryMBR = queryShape.getMBR();
      toBeSearched.push(0); // Start from the root
      this.resultShape = (T) stockObject.clone();
      this.nextResultShape = (T) stockObject.clone();
      prepareNextResult();
    }
    
    @Override
    public Iterator<T> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return nextResultShape != null;
    }

    @Override
    public T next() {
      T temp = resultShape;
      resultShape = nextResultShape;
      nextResultShape = temp;
      prepareNextResult();
      return resultShape;
    }
    
    @Override
    public void remove() {
      throw new RuntimeException("Unsupported method");
    }

    /**
     * Search for next item in result and store it in resultShape.
     * If no more results found, set resultShape to null
     */
    protected void prepareNextResult() {
      try {
        while (lineReader != null && firstOffset < lastOffset) {
          // Case 1: Searching within a node
          firstOffset += lineReader.readLine(line);
          nextResultShape.fromText(line);
          if (nextResultShape.isIntersected(queryShape)) {
            return;
          }
        }
        // Case 2: Searching in nodes
        while (!toBeSearched.isEmpty()) {
          int searchNumber = toBeSearched.pop();

          if (searchNumber < nodeCount) {
            // Searching a node
            int nodeID = searchNumber;
            if (queryMBR.isIntersected(nodes[nodeID])) {
              boolean is_leaf = nodeID >= nonLeafNodeCount;
              if (is_leaf) {
                // Check all objects under this node
                int start_offset = RTree.this.dataOffset[nodeID];
                int end_offset = RTree.this.dataOffset[nodeID + 1];
                toBeSearched.add(start_offset);
                toBeSearched.add(end_offset);
              } else {
                // Add all child nodes
                for (int iChild = 0; iChild < RTree.this.degree; iChild++) {
                  toBeSearched.add(nodeID * RTree.this.degree + iChild + 1);
                }
              }
            }
          } else {
            // searchNumber is the end offset of data search. Start offset is next
            // in stack
            lastOffset = searchNumber;
            firstOffset = toBeSearched.pop();

            data.seek(firstOffset + treeStartOffset);
            lineReader = new LineReader(data);
            while (firstOffset < lastOffset) {
              firstOffset += lineReader.readLine(line);
              nextResultShape.fromText(line);
              if (nextResultShape.isIntersected(queryShape)) {
                return;
              }
            }
          }
        }
        // No more results in the tree
        nextResultShape = null;
        
      } catch (IOException e) {
        e.printStackTrace();
        nextResultShape = null;
      }
    }
    
  }
  
  /**
   * Searches the tree for all shapes overlapping the queryShape and returns
   * an iterator for all these shapes
   * @param queryShape
   * @return
   */
  public Iterable<T> search(Shape queryShape) {
    return new SearchIterator(queryShape);
  }

  
  /**
   * Performs a range query over this tree using the given query range.
   * @param query - The query rectangle to use (TODO make it any shape not just rectangle)
   * @param output - Shapes found are reported to this output. If null, results are not reported
   * @return - Total number of records found
   */
  public int search(Shape query, ResultCollector<T> output) {
    int resultCount = 0;
    
    try {
      resultCount = search(query, output, 0, 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return resultCount;
  }
  
  /**
   * k nearest neighbor query
   * @param qx
   * @param qy
   * @param k
   * @param output
   */
  public int knn(final double qx, final double qy, int k, final ResultCollector2<T, Double> output) {
    double query_area = ((getMBR().x2 - getMBR().x1) * (getMBR().y2 - getMBR().y1)) * k / getElementCount();
    double query_radius = Math.sqrt(query_area / Math.PI);

    boolean result_correct;
    final Vector<Double> distances = new Vector<Double>();
    final Vector<T> shapes = new Vector<T>();
    // Find results in the range and increase this range if needed to ensure
    // correctness of the answer
    do {
      // Initialize result and query range
      distances.clear(); shapes.clear();
      Rectangle queryRange = new Rectangle();
      queryRange.x1 = qx - query_radius;
      queryRange.y1 = qy - query_radius;
      queryRange.x2 = qx + query_radius;
      queryRange.y2 = qy + query_radius;
      // Retrieve all results in range
      search(queryRange, new ResultCollector<T>() {
        @Override
        public void collect(T shape) {
          distances.add(shape.distanceTo(qx, qy));
          shapes.add((T) shape.clone());
        }
      });
      if (shapes.size() < k) {
        // Didn't find k elements in range, double the range to get more items
        if (shapes.size() == getElementCount()) {
          // Already returned all possible elements
          result_correct = true;
        } else {
          query_radius *= 2;
          result_correct = false;
        }
      } else {
        // Sort items by distance to get the kth neighbor
        IndexedSortable s = new IndexedSortable() {
          @Override
          public void swap(int i, int j) {
            double temp_distance = distances.elementAt(i);
            distances.set(i, distances.elementAt(j));
            distances.set(j, temp_distance);
            
            T temp_shape = shapes.elementAt(i);
            shapes.set(i, shapes.elementAt(j));
            shapes.set(j, temp_shape);
          }
          @Override
          public int compare(int i, int j) {
            // Note. Equality is not important to check because items with the
            // same distance can be ordered anyway. 
        	if (distances.elementAt(i) == distances.elementAt(j))
                  return 0;
        	if (distances.elementAt(i) < distances.elementAt(j))
              return -1;
            return 1;
          }
        };
        IndexedSorter sorter = new QuickSort();
        sorter.sort(s, 0, shapes.size());
        if (distances.elementAt(k - 1) > query_radius) {
          result_correct = false;
          query_radius = distances.elementAt(k);
        } else {
          result_correct = true;
        }
      }
    } while (!result_correct);
    
    int result_size = Math.min(k,  shapes.size());
    if (output != null) {
      for (int i = 0; i < result_size; i++) {
        output.collect(shapes.elementAt(i), distances.elementAt(i));
      }
    }
    return result_size;
  }

  protected static<S1 extends Shape, S2 extends Shape> int spatialJoinMemory(
      final RTree<S1> R,
      final RTree<S2> S,
      final ResultCollector2<S1, S2> output,
      final Reporter reporter)
      throws IOException {
    S1[] rs = (S1[]) Array.newInstance(R.stockObject.getClass(), R.getElementCount());
    int i = 0;
    for (S1 r : R)
      rs[i++] = (S1) r.clone();
    if (i != rs.length)
      throw new RuntimeException(i+"!="+rs.length);
    
    S2[] ss = (S2[]) Array.newInstance(S.stockObject.getClass(), S.getElementCount());
    i = 0;
    for (S2 s : S)
      ss[i++] = (S2) s.clone();
    if (i != ss.length)
      throw new RuntimeException(i+"!="+ss.length);

    return SpatialAlgorithms.SpatialJoin_planeSweep(rs, ss, output, reporter);
  }
  
  //LRU cache used to avoid deserializing the same records again and again
  static class LruCache<A, B> extends LinkedHashMap<A, B> {
    private static final long serialVersionUID = 702044567572914544L;
    private final int maxEntries;
    private B unusedEntry;
  
    public LruCache(final int maxEntries) {
      super(maxEntries + 1, 1.0f, true);
      this.maxEntries = maxEntries;
    }
  
    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
      if (super.size() > maxEntries) {
        unusedEntry = eldest.getValue();
        return true;
      }
      return false;
    }
  
    public B popUnusedEntry() {
      B temp = unusedEntry;
      unusedEntry = null;
      return temp;
    }
  }

  /**
   * Performs a spatial join between records in two R-trees
   * @param R
   * @param S
   * @param output
   * @return
   * @throws IOException
   * SuppresWarnings("resource") is used because we create LineReaders on the
   * internal data stream of both R and S. We do not want to close the
   * LineReader because it will subsequently close the internal data stream
   * of R and S which is something we want to avoid because both R and S are
   * not created by this function and it should not free these resources.
   */
  protected static<S1 extends Shape, S2 extends Shape> int spatialJoinDisk(
      final RTree<S1> R,
      final RTree<S2> S,
      final ResultCollector2<S1, S2> output,
      final Reporter reporter)
      throws IOException {
    PriorityQueue<Long> nodesToJoin = new PriorityQueue<Long>(R.nodeCount+S.nodeCount);
    
    // Start with the two roots
    nodesToJoin.add(0L);

    // Caches to keep the retrieved data records. Helpful when it reaches the
    // leaves and starts to read objects from the two trees
    LruCache<Integer, Shape[]> r_records_cache = new LruCache<Integer, Shape[]>(
        R.degree * 2);
    LruCache<Integer, Shape[]> s_records_cache = new LruCache<Integer, Shape[]>(
        S.degree * R.degree * 4);
    
    Text line = new Text2();
    
    int result_count = 0;

    LineReader r_lr = null, s_lr = null;
    // Last offset read from r and s
    int r_last_offset = 0;
    int s_last_offset = 0;
    
    while (!nodesToJoin.isEmpty()) {
      long nodes_to_join = nodesToJoin.remove();
      int r_node = (int) (nodes_to_join >>> 32);
      int s_node = (int) (nodes_to_join & 0xFFFFFFFF);
      
      // Compute the overlap between the children of the two nodes
      // If a node is non-leaf, its children are other nodes
      // If a node is leaf, its children are data records
      boolean r_leaf = r_node >= R.nonLeafNodeCount;
      boolean s_leaf = s_node >= S.nonLeafNodeCount;
      
      if (!r_leaf && !s_leaf) {
        // Both are internal nodes, read child nodes under them
        // Find overlaps using a simple cross join (TODO: Use plane-sweep)
        for (int i = 0; i < R.degree; i++) {
          int new_r_node = r_node * R.degree + i + 1;
          for (int j = 0; j < S.degree; j++) {
            int new_s_node = s_node * S.degree + j + 1;
            if (R.nodes[new_r_node].isIntersected(S.nodes[new_s_node])) {
              long new_pair = (((long)new_r_node) << 32) | new_s_node;
              nodesToJoin.add(new_pair);
            }
          }
        }
      } else if (r_leaf && !s_leaf) {
        // R is a leaf node while S is an internal node
        // Compare the leaf node in R against all child nodes of S
        for (int j = 0; j < S.degree; j++) {
          int new_s_node = s_node * S.degree + j + 1;
          if (R.nodes[r_node].isIntersected(S.nodes[new_s_node])) {
            long new_pair = (((long)r_node) << 32) | new_s_node;
            nodesToJoin.add(new_pair);
          }
        }
      } else if (!r_leaf && s_leaf) {
        // R is an internal node while S is a leaf node
        // Compare child nodes of R against the leaf node in S
        for (int i = 0; i < R.degree; i++) {
          int new_r_node = r_node * R.degree + i + 1;
          if (R.nodes[new_r_node].isIntersected(S.nodes[s_node])) {
            long new_pair = (((long)new_r_node) << 32) | s_node;
            nodesToJoin.add(new_pair);
          }
        }
      } else if (r_leaf && s_leaf) {
        // Both are leaf nodes, join objects under them
        int r_start_offset = R.dataOffset[r_node];
        int r_end_offset = R.dataOffset[r_node+1];
        int s_start_offset = S.dataOffset[s_node];
        int s_end_offset = S.dataOffset[s_node+1];
        
        // Read or retrieve r_records
        Shape[] r_records = r_records_cache.get(r_start_offset);
        if (r_records == null) {
          int cache_key = r_start_offset;
          r_records = r_records_cache.popUnusedEntry();
          if (r_records == null) {
            r_records = new Shape[R.degree * 2];
          }

          // Need to read it from stream
          if (r_last_offset != r_start_offset) {
            long seekTo = r_start_offset + R.treeStartOffset;
            R.data.seek(seekTo);
            r_lr = new LineReader(R.data);
          }
          int record_i = 0;
          while (r_start_offset < r_end_offset) {
            r_start_offset += r_lr.readLine(line);
            if (r_records[record_i] == null)
              r_records[record_i] = R.stockObject.clone();
            r_records[record_i].fromText(line);
            record_i++;
          }
          r_last_offset = r_start_offset;
          // Nullify other records
          while (record_i < r_records.length)
            r_records[record_i++] = null;
          r_records_cache.put(cache_key, r_records);
        }

        // Read or retrieve s_records
        Shape[] s_records = s_records_cache.get(s_start_offset);
        if (s_records == null) {
          int cache_key = s_start_offset;

          // Need to read it from stream
          if (s_lr == null || s_last_offset != s_start_offset) {
            // Need to reposition s_lr (LineReader of S)
            long seekTo = s_start_offset + S.treeStartOffset;
            S.data.seek(seekTo);
            s_lr = new LineReader(S.data);
          }
          s_records = s_records_cache.popUnusedEntry();
          if (s_records == null) {
            s_records = new Shape[S.degree * 2];
          }
          int record_i = 0;
          while (s_start_offset < s_end_offset) {
            s_start_offset += s_lr.readLine(line);
            if (s_records[record_i] == null)
              s_records[record_i] = S.stockObject.clone();
            s_records[record_i].fromText(line);
            record_i++;
          }
          // Nullify other records
          while (record_i < s_records.length)
            s_records[record_i++] = null;
          // Put in cache
          s_records_cache.put(cache_key, s_records);
          s_last_offset = s_start_offset;
        }

        // Do Cartesian product between records to find overlapping pairs
        for (int i_r = 0; i_r < r_records.length && r_records[i_r] != null; i_r++) {
          for (int i_s = 0; i_s < s_records.length && s_records[i_s] != null; i_s++) {
            if (r_records[i_r].isIntersected(s_records[i_s]) &&
                !r_records[i_r].equals(s_records[i_s])) {
              result_count++;
              if (output != null) {
                output.collect((S1)r_records[i_r], (S2)s_records[i_s]);
              }
            }
          }
        }
      }
      if (reporter != null)
        reporter.progress();
    }
    return result_count;
  }
  
  public static<S1 extends Shape, S2 extends Shape> int spatialJoin(
      final RTree<S1> R,
      final RTree<S2> S,
      final ResultCollector2<S1, S2> output,
      final Reporter reporter)
      throws IOException {
    try {
      if (R.treeStartOffset >= 0 && S.treeStartOffset >= 0) {
        // Both trees are read from disk
        return spatialJoinDisk(R, S, output, reporter);
      } else {
        return spatialJoinMemory(R, S, output, reporter);
      }
    } catch (TopologyException e) {
      e.printStackTrace();
      return 0;
    }
  }
  /**
   * Calculate the storage overhead required to build an RTree for the given
   * number of nodes.
   * @return - storage overhead in bytes
   */
  public static int calculateStorageOverhead(int elementCount, int degree){
    // Update storage overhead
    int height = Math.max(1, 
        (int) Math.ceil(Math.log(elementCount)/Math.log(degree)));
    int leafNodeCount = (int) Math.pow(degree, height - 1);
    if (elementCount <=  2 * leafNodeCount && height > 1) {
      height--;
      leafNodeCount = (int) Math.pow(degree, height - 1);
    }
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    int storage_overhead = 4 + TreeHeaderSize + nodeCount * NodeSize;
    return storage_overhead;
  }

  /**
   * Find log to the base 2 quickly
   * @param x
   * @return
   */
  public static int log2Floor(int x) {
    if (x == 0)
      return -1;
    int pos = 0;
    if ((x & 0xFFFF0000) != 0) {
      pos += 16;
      x >>>= 16;
    }
    if ((x & 0xFF00) != 0) {
      pos += 8;
      x >>>= 8;
    }
    if ((x & 0xF0) != 0) {
      pos += 4;
      x >>>= 4;
    }
    if ((x & 0xC) != 0) {
      pos += 2;
      x >>>= 2;
    }
    if ((x & 0x2) != 0) {
      pos++;
      x >>>= 1;
    }
    
    return pos;
  }
  
  public static int powInt(int base, int exponent) {
    int pow = 1;
    while (exponent != 0) {
      if ((exponent & 1) != 0)
        pow *= base;
      exponent >>>= 1;
      base *= base;
    }
    return pow;
  }
  
  private static final double LogLookupTable[];

  static {
    int count = 100;
    LogLookupTable = new double[count];
    for (int i = 0; i < count; i++) {
      LogLookupTable[i] = Math.log(i);
    }
  }
  
  public static double fastLog(int x) {
    if (x < LogLookupTable.length) {
      return LogLookupTable[x];
    }
    return Math.log(x);
  }
  
  public static double fastPow(double a, double b) {
    final long tmp = (long) (9076650 * (a - 1) / (a + 1 + 4 * (Math.sqrt(a))) * b + 1072632447);
    return Double.longBitsToDouble(tmp << 32);
  }
  
  /**
   * Find the best (minimum) degree that can index the given number of records
   * such that the whole tree structure can be stored in the given bytes
   * available.
   * @param bytesAvailable
   * @param recordCount
   * @return
   */
  public static int findBestDegree(int bytesAvailable, int recordCount) {
    // Maximum number of nodes that can be stored in the bytesAvailable
    int maxNodeCount = (bytesAvailable - TreeHeaderSize) / NodeSize;
    // Calculate maximum possible tree height to store the given record count
    int h_max = log2Floor(recordCount / 2);
    // Minimum height is always 1 (degree = recordCount)
    int h_min = 2;
    // Best degree is the minimum degree
    int d_best = Integer.MAX_VALUE;
    double log_recordcount_e = Math.log(recordCount/2);
    double log_recordcount_2 = log_recordcount_e/fastLog(2);
    // Find the best height among all possible heights
    for (int h = h_min; h <= h_max; h++) {
      // Find the minimum degree for the given height (h)
      // This approximation is good enough for our case.
      // Not proven but tested with millions of random cases
      int d_min = (int) Math.ceil(fastPow(2.0, log_recordcount_2 /(h+1)));
      // Some heights are invalid, recalculate the height to ensure it's valid
      int h_recalculated = (int) Math.floor(log_recordcount_e/fastLog(d_min));
      if (h != h_recalculated)
        continue;
      int nodeCount = (int) ((powInt(d_min, h+1)-1) / (d_min-1));
      if (nodeCount < maxNodeCount && d_min < d_best)
        d_best = d_min;
    }
    
    return d_best;
  }
  
  public static int calculateTreeStorage(int elementCount, int degree) {
    int height = Math.max(1, 
        (int) Math.ceil(Math.log(elementCount)/Math.log(degree)));
    int leafNodeCount = (int) Math.pow(degree, height - 1);
    if (elementCount <  2 * leafNodeCount && height > 1) {
      height--;
      leafNodeCount = (int) Math.pow(degree, height - 1);
    }
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    return TreeHeaderSize + nodeCount * NodeSize;
  }

  @Override
  public void close() throws IOException {
    if (data != null)
      data.close();
  }
  
  public void toWKT(PrintStream out) throws IOException {
    out.println("NodeID\tBoundaries");
    for (int nodeID = 0; nodeID < this.nodeCount; nodeID++) {
      out.printf("%d\t%s\n", nodeID, nodes[nodeID].toWKT());
    }
  }

  /**
   * A main method that creates a single R-tree out of a single file.
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput())
      throw new RuntimeException("Input-output combination not correct");
    Path inPath = params.getInputPath();
    Path outPath = params.getOutputPath();
    Shape shape = params.getShape("shape");

    // Read the whole input file as one byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024*1024];
    FileSystem inFS = inPath.getFileSystem(params);
    FSDataInputStream in = inFS.open(inPath);
    int bytesRead;
    while ((bytesRead = in.read(buffer)) >= 0) {
      baos.write(buffer, 0, bytesRead);
    }
    in.close();
    baos.close();
    
    // Create the R-tree and write to output
    byte[] inputData = baos.toByteArray();
    FileSystem outFS = outPath.getFileSystem(params);
    FSDataOutputStream out = outFS.create(outPath);
    out.write(SpatialSite.RTreeFileMarkerB);
    RTree.bulkLoadWrite(inputData, 0, inputData.length, 4, out, shape, true);
    out.close();
  }
}
