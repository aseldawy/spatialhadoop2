package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A class that stores an auxiliary data structure used to search through
 * the partitions created using the functions
 * {@link RStarTree#partitionPoints(double[], double[], int, int, boolean, AuxiliarySearchStructure)}
 * and
 * {@link RRStarTree#partitionPoints(double[], double[], int, int, boolean, AuxiliarySearchStructure)}
 */
public class AuxiliarySearchStructure implements Writable {
  /**The first split to consider*/
  public int rootSplit;
  /**The coordinate along where the split happens*/
  public double[] splitCoords;
  /**The axis along where the partition happened. 0 for X and 1 for Y.*/
  public BitArray splitAxis;
  /**
   * The next partition to consider if the search point is less than the
   * split line. If the number is negative, it indicates that the search is
   * terminated and a partition is matched. The partition index is (-x-1)
   **/
  public int[] partitionLessThan;
  /**
   * The next partition to consider if the search point is greater than or
   * equal to the split line.
   */
  public int[] partitionGreaterThanOrEqual;

  /**
   * Returns the ID of the partition that contain the given point
   * @param x
   * @param y
   * @return
   */
  public int search(double x, double y) {
    if (splitCoords.length == 0)
      return 0;
    int iSplit = rootSplit;
    while (iSplit >= 0) {
      // Choose which coordinate to work with depending on the split axis
      double coordToConsider = splitAxis.get(iSplit)? y : x;
      iSplit = (coordToConsider < splitCoords[iSplit] ? partitionLessThan : partitionGreaterThanOrEqual)[iSplit];
    }
    // Found the terminal partition, return its correct ID
    return -iSplit - 1;
  }

  /**
   * Find all the overlapping partitions for a given rectangular area
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @param ps
   */
  public void search(double x1, double y1, double x2, double y2, IntArray ps) {
    // We to keep a stack of splits to consider.
    // For efficiency, we reuse the given IntArray (ps) where we store the
    // the matching partitions from one end and the splits to be considered
    // from the other end
    // A negative number indicates a matching partition
    ps.clear();
    if (splitCoords.length == 0) {
      // No splits. Always return 0 which is the only partition we have
      ps.add(0);
      return;
    }
    IntArray splitsToSearch = ps;
    int iSplit = rootSplit;
    while (iSplit >= 0) {
      double coordMin = splitAxis.get(iSplit)? y1 : x1;
      double coordMax = splitAxis.get(iSplit)? y2 : x2;
      if (coordMax < splitCoords[iSplit]) {
        // Only the first half-space matches
        iSplit = partitionLessThan[iSplit];
      } else if (coordMin >= splitCoords[iSplit]) {
        // Only the second half-space matches
        iSplit = partitionGreaterThanOrEqual[iSplit];
      } else {
        // The entire space is still to be considered
        if (partitionGreaterThanOrEqual[iSplit] >= 0) {
          // A split that needs to be further considered
          splitsToSearch.add(partitionGreaterThanOrEqual[iSplit]);
        } else {
          // A terminal partition that should be matched
          splitsToSearch.insert(0, partitionGreaterThanOrEqual[iSplit]);
        }
        iSplit = partitionLessThan[iSplit];
      }
      // If iSplit reaches a terminal partitions, add it to the answer and
      // move on to the next split
      while (iSplit < 0) {
        ps.insert(0, iSplit);
        if (splitsToSearch.peek() >= 0)
          iSplit = splitsToSearch.pop();
        else
          break;
      }
    }
    // Convert the matching splits from their negative number to the correct
    // partitionID
    for (int i = 0; i < ps.size(); i++)
      ps.set(i, -ps.get(i) - 1);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(rootSplit);
    out.writeInt(splitCoords.length);
    for (int i = 0; i < splitCoords.length; i++) {
      out.writeDouble(splitCoords[i]);
      out.writeInt(partitionLessThan[i]);
      out.writeInt(partitionGreaterThanOrEqual[i]);
    }
    splitAxis.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rootSplit = in.readInt();
    int numOfSplits = in.readInt();
    if (splitCoords == null) splitCoords = new double[numOfSplits];
    if (partitionLessThan == null) partitionLessThan = new int[numOfSplits];
    if (partitionGreaterThanOrEqual == null) partitionGreaterThanOrEqual = new int[numOfSplits];
    for (int i = 0; i < numOfSplits; i++) {
      splitCoords[i] = in.readDouble();
      partitionLessThan[i] = in.readInt();
      partitionGreaterThanOrEqual[i] = in.readInt();
    }
    if (splitAxis == null) splitAxis = new BitArray();
    splitAxis.readFields(in);
  }
}