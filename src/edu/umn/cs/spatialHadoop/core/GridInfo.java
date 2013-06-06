package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * Stores grid information that can be used with spatial files.
 * The grid is uniform which means all cells have the same width and the same
 * height.
 * @author Ahmed Eldawy
 *
 */
public class GridInfo extends Rectangle {
  public int columns, rows;

  public GridInfo() {
  }
  
  public GridInfo(double x1, double y1, double x2, double y2) {
    super(x1, y1, x2, y2);
    this.columns = 0;
    this.rows = 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(columns);
    out.writeInt(rows);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    columns = in.readInt();
    rows = in.readInt();
  }

  @Override
  public String toString() {
    return "grid: "+x1+","+y1+","+x2+","+y2+", " +
    "cell: "+getAverageCellWidth()+","+getAverageCellHeight()+
    "("+columns+"x"+rows+")";
  }
  
  public double getAverageCellHeight() {
    return (y2 - y1) / Math.max(rows, 1);
  }

  public double getAverageCellWidth() {
    return (x2 - x1) / Math.max(columns, 1);
  }

  @Override
  public boolean equals(Object obj) {
    GridInfo gi = (GridInfo) obj;
    return super.equals(obj)
        && this.columns == gi.columns && this.rows == gi.rows;
  }
  
  public void calculateCellDimensions(long totalFileSize, long blockSize) {
    // An empirical number for the expected overhead in grid file due to
    // replication
    int numBlocks = (int) Math.ceil((double)totalFileSize / blockSize);
    calculateCellDimensions(numBlocks);
  }
  
  public void calculateCellDimensions(int numCells) {
    int gridCols = 1;
    int gridRows = 1;
    while (gridRows * gridCols < numCells) {
      // (  cellWidth          >    cellHeight        )
      if ((x2 - x1) / gridCols > (y2 - y1) / gridRows) {
        gridCols++;
      } else {
        gridRows++;
      }
    }
    columns = gridCols;
    rows = gridRows;
  }

  @Override
  public Text toText(Text text) {
    final byte[] Comma = ",".getBytes();
    super.toText(text);
    text.append(Comma, 0, Comma.length);
    TextSerializerHelper.serializeLong(columns, text, ',');
    TextSerializerHelper.serializeLong(rows, text, '\0');
    return text;
  }

  @Override
  public void fromText(Text text) {
    super.fromText(text);
    if (text.getLength() > 0) {
      // Remove the first comma
      System.arraycopy(text.getBytes(), 1, text.getBytes(), 0, text.getLength() - 1);
      columns = (int) TextSerializerHelper.consumeInt(text, ',');
      rows = (int) TextSerializerHelper.consumeInt(text, '\0');
    }
  }

  public CellInfo[] getAllCells() {
    int cellIndex = 0;
    CellInfo[] cells = new CellInfo[columns * rows];
    double xstart = x1;
    for (int col = 0; col < columns; col++) {
      double xend = x1 + (x2 - x1) * (col+1) / columns;
      
      double ystart = y1;
      for (int row = 0; row < rows; row++) {
        double yend = y1 + (y2 - y1) * (row+1) / rows;
        cells[cellIndex] = new CellInfo(cellIndex, xstart, ystart, xend, yend);
        cellIndex++;
        
        ystart = yend;
      }
      xstart = xend;
    }
    return cells;
  }

}
