package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.Iterator;

/**
 * A local index that uses {@link RRStarTree} for building the index and
 * {@link RTreeGuttman} for reading it.
 * @param <S>
 */
@LocalIndex.LocalIndexMetadata(extension = "rrstar")
public class RRStarLocalIndex<S extends Shape> implements LocalIndex<S> {

  /**The configuration of the underlying job*/
  private Configuration conf;

  /**
   * Number of nodes to fit in one disk page.
   * One node consists of four bytes for the number of children,
   * 32 bytes (four double coordinates for the MBR), and
   * four bytes for the offset of each child in the file.
   */
  private static final int MaxCapacity = 4096 / (4 + 8 * 4 + 4);

  /**The underlying R-tree used when reading the index from disk*/
  protected RTreeGuttman underlyingRTree;

  protected DataIterator<S> iter;

  /**The start and end offsets of the data chunk*/
  protected long dataStart, dataEnd;

  @Override
  public void setup(Configuration conf) {
    this.conf = conf;
    this.iter = new DataIterator<S>();
  }

  @Override
  public void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile, S shape) throws IOException, InterruptedException {
    // Read all data of the written file in memory as one byte array
    final byte[] cellData = new byte[(int) nonIndexedFile.length()];
    InputStream cellIn = new BufferedInputStream(new FileInputStream(nonIndexedFile));
    cellIn.read(cellData);
    cellIn.close();

    // Count number of lines in the file, i.e., number of records, and the
    // start of each record
    final IntArray recordOffsets = new IntArray();
    int i1 = 0;
    while (i1 < cellData.length) {
      // i1 should point to the first character in a record
      recordOffsets.add(i1);
      // i2 should point the first character in the next record
      int i2 = i1 + 1;
      // Skip until the first EOL character
      while (i2 < cellData.length && cellData[i2] != '\n' && cellData[i2] != '\r')
        i2++;
      // Skip any additional EOL characters
      while (i2 < cellData.length && (cellData[i2] == '\n' || cellData[i2] == '\r'))
        i2++;
      i1 = i2;
    }
    // To simplify the calculations, always append the end offset of the array
    recordOffsets.add(i1);

    // Scan the array again but this time compute the MBRs of objects
    int numRecords = recordOffsets.size() - 1;
    double[] x1s = new double[numRecords];
    double[] y1s = new double[numRecords];
    double[] x2s = new double[numRecords];
    double[] y2s = new double[numRecords];

    Text line = new Text2();
    for (int i = 0; i < numRecords; i++) {
      line.set(cellData, recordOffsets.get(i), recordOffsets.get(i+1)-recordOffsets.get(i));
      shape.fromText(line);
      Rectangle mbr = shape.getMBR();
      x1s[i] = mbr.x1;
      y1s[i] = mbr.y1;
      x2s[i] = mbr.x2;
      y2s[i] = mbr.y2;
    }

    // Now, it is time to build the tree
    RRStarTree rtree = new RRStarTree(MaxCapacity * 2 / 10, MaxCapacity);
    rtree.initializeFromRects(x1s, y1s, x2s, y2s);

    // The tree is built, write it to the output
    FileSystem outFS = outputIndexedFile.getFileSystem(conf);
    FSDataOutputStream out = outFS.create(outputIndexedFile);
    rtree.write(out, new RTreeGuttman.Serializer() {
      @Override
      public int serialize(DataOutput out, int iObject) throws IOException {
        out.write(cellData, recordOffsets.get(iObject),
            recordOffsets.get(iObject+1) - recordOffsets.get(iObject));
        return recordOffsets.get(iObject+1) - recordOffsets.get(iObject);
      }
    });
    int indexSize = (int) out.getPos();
    out.writeInt(indexSize);
    out.close();
  }

  @Override
  public long getDataStart() {
    return dataStart;
  }

  @Override
  public long getDataEnd() {
    return dataEnd;
  }

  @Override
  public void read(FSDataInputStream in, long start, long end, final S mutableShape) throws IOException {
    if (underlyingRTree == null)
      underlyingRTree = new RTreeGuttman(1, 2);
    underlyingRTree.readFields(in, end - start - 4, new RTreeGuttman.Deserializer<S>() {
      private Text line = new Text2();
      @Override
      public S deserialize(DataInput in, int length) throws IOException {
        // Data is written in text format
        line.clear();
        line.readWithKnownLength(in, length);
        mutableShape.fromText(line);
        return mutableShape;
      }
    });
    dataStart = start;
    dataEnd = start + underlyingRTree.getTotalDataSize();
  }

  @Override
  public Iterable<? extends S> search(double x1, double y1, double x2, double y2) {
    Iterable<RTreeGuttman.Entry> treeResults = underlyingRTree.search(x1, y1, x2, y2);
    iter.wrap(treeResults);
    return iter;
  }

  @Override
  public Iterable<? extends S> scanAll() {
    Iterable<RTreeGuttman.Entry> allElements = underlyingRTree.entrySet();
    iter.wrap(allElements);
    return iter;
  }

  @Override
  public void close() throws IOException {
    underlyingRTree.close();
  }

  static class DataIterator<S> implements Iterable<S>, Iterator<S> {

    private Iterable<RTreeGuttman.Entry> underlyingIterable;
    private Iterator<RTreeGuttman.Entry> underlyingIterator;

    public void wrap(Iterable<RTreeGuttman.Entry> i) {
      this.underlyingIterable = i;
    }

    @Override
    public Iterator<S> iterator() {
      this.underlyingIterator = this.underlyingIterable.iterator();
      return this;
    }

    @Override
    public boolean hasNext() {
      return underlyingIterator.hasNext();
    }

    @Override
    public S next() {
      RTreeGuttman.Entry e = underlyingIterator.next();
      try {
        return (S) e.getObject();
      } catch (IOException e1) {
        e1.printStackTrace();
        return null;
      }
    }

    public void remove() {
      throw new RuntimeException("Not implemented");
    }
  }

}
