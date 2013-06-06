package edu.umn.cs.spatialHadoop.core;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.operations.FileMBR;

/**
 * 
 * @author eldawy
 * @deprecated - Use Repartition class instead
 */
@Deprecated
public class WriteGridFile {
  public static final Log LOG = LogFactory.getLog(WriteGridFile.class);

  /**
   * Writes a grid file to HDFS.
   * 
   * @param inFileSystem
   * @param inputPath
   * @param outFileSystem
   * @param outputPath
   * @param gridInfo
   * @param shapeClass
   * @param pack
   *          - set to <code>true</code> to pack grid cells in a
   *          distribution-aware manner
   * @param rtree
   *          - set to <code>true</code> to store each grid cell as rtree
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static<S extends Shape> void writeGridFile(FileSystem inFileSystem,
      Path inputPath, FileSystem outFileSystem, Path outputPath, GridInfo gridInfo,
      S shape, boolean pack, boolean rtree, boolean overwrite)
      throws IOException {
    gridInfo = calculateGridInfo(inFileSystem, inputPath, outFileSystem, gridInfo, shape);
    if (rtree)
      gridInfo.calculateCellDimensions(inFileSystem.getFileStatus(inputPath).getLen() * 4, outFileSystem.getDefaultBlockSize());
    CellInfo[] cells = pack ? packInRectangles(inFileSystem, inputPath, outFileSystem, gridInfo, shape) :
      gridInfo.getAllCells();
    
    if (outFileSystem.exists(outputPath)) {
      if (overwrite)
        outFileSystem.delete(outputPath, true);
      else
        throw new RuntimeException("Output file already exists and -overwrite flag is not set");
    }
    outFileSystem.mkdirs(outputPath);

    // Prepare grid file writer
    ShapeRecordWriter<S> rrw = rtree ?
        new RTreeGridRecordWriter<S>(outputPath, null, null, cells, false):
        new GridRecordWriter<S>(outputPath, null, null, cells, false);

    // Open input file
    LineReader reader = new LineReader(inFileSystem.open(inputPath));

    Text line = new Text();
    while (reader.readLine(line) > 0) {
      // Parse shape dimensions
      shape.fromText(line);

      // Write to output file
      rrw.write(NullWritable.get(), shape);
    }
    
    // Close input file
    reader.close();

    // Close output file
    rrw.close(null);
  }
  
  /**
   * Calculate minimum bounding rectangle of a file by opening it and reading every tuple.
   * @param fileSystem
   * @param path
   * @return
   * @throws IOException
   */
  private static<S extends Shape> Rectangle calculateMBR(FileSystem fileSystem, Path path, S shape) throws IOException {
    LineReader inFile = new LineReader(fileSystem.open(path));
    Rectangle rectangle = new Rectangle();
    Text line = new Text();
    double x1 = Long.MAX_VALUE;
    double x2 = Long.MIN_VALUE;
    double y1 = Long.MAX_VALUE;
    double y2 = Long.MIN_VALUE;
    while (inFile.readLine(line) > 0) {
      // Parse shape dimensions
      shape.fromText(line);

      if (rectangle.x2 < x1) x1 = rectangle.x1;
      if (rectangle.y1 < y1) y1 = rectangle.y1;
      if (rectangle.x2 > x2) x2 = rectangle.x2;
      if (rectangle.y2 > y2) y2 = rectangle.y2;
    }

    inFile.close();
    return new Rectangle(x1, y1, x2, y2);
  }
  
  /**
   * Returns MBR of a file. If the file is a grid file, MBR is calculated from GridInfo
   * associated with the file. With a heap file, the file is parsed, and MBR is calculated.
   * @param fileSystem
   * @param path
   * @return
   * @throws IOException
   */
  public static <S extends Shape> Rectangle getMBR(FileSystem fileSystem,
      Path path, S shape) throws IOException {
    return FileMBR.fileMBR(fileSystem, path, shape);
  }
  
  /**
   * Calculate GridInfo to be used to write a file using a heuristic.
   * @param inFileSystem
   * @param path
   * @return
   * @throws IOException
   */
  public static<S extends Shape> GridInfo calculateGridInfo(FileSystem inFileSystem, Path path,
      FileSystem outFileSystem, GridInfo gridInfo, S shape) throws IOException {
    if (gridInfo == null) {
      Rectangle mbr = getMBR(inFileSystem, path, shape);
      LOG.info("MBR: "+mbr);
      gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      LOG.info("GridInfo: "+gridInfo);
    }
    if (gridInfo.columns == 0) {
      gridInfo.calculateCellDimensions(inFileSystem.getFileStatus(path).getLen(), outFileSystem.getDefaultBlockSize());
      LOG.info("GridInfo: "+gridInfo);
    }
    return gridInfo;
  }
  
  private static final int MaxLineLength = 200;
  
  public static <S extends Shape> CellInfo[] packInRectangles(
      FileSystem inFileSystem, Path inputPath, FileSystem outFileSystem,
      GridInfo gridInfo, S shape) throws IOException {
    return packInRectangles(inFileSystem, new Path[] { inputPath },
        outFileSystem, gridInfo, shape);
  }

  public static<S extends Shape> CellInfo[] packInRectangles(FileSystem inFileSystem, Path[] inputPaths,
      FileSystem outFileSystem, GridInfo gridInfo, S shape) throws IOException {
    Point[] samples = pickRandomSample(inFileSystem, inputPaths, shape);
    Rectangle[] rectangles = RTree.packInRectangles(gridInfo, samples);
    CellInfo[] cellsInfo = new CellInfo[rectangles.length];
    for (int i = 0; i < rectangles.length; i++)
      cellsInfo[i] = new CellInfo(i, rectangles[i]);
    return cellsInfo;
  }

  private static<S extends Shape> Point[] pickRandomSample(FileSystem inFileSystem,
      Path[] inputPaths, S shape) throws IOException {
    LOG.info("Picking a random sample from file");
    Random random = new Random();
    long inTotalSize = 0;
    long[] inputSize = new long[inputPaths.length];
    for (int fileIndex = 0; fileIndex < inputPaths.length; fileIndex++) {
      inputSize[fileIndex] = inFileSystem.getFileStatus(inputPaths[fileIndex]).getLen();
      inTotalSize += inputSize[fileIndex];
    }

    List<Point> samplePoints = new ArrayList<Point>();

    long totalSampleSize = inTotalSize / 500;
    LOG.info("Going to pick a sample of size: "+totalSampleSize+" bytes");
    long totalBytesToSample = totalSampleSize;
    long lastTime = System.currentTimeMillis();

    FSDataInputStream[] inputStreams = new FSDataInputStream[inputPaths.length];
    for (int fileIndex = 0; fileIndex < inputPaths.length; fileIndex++) {
      inputStreams[fileIndex] = inFileSystem.open(inputPaths[fileIndex]);
    }
    
    Text text = new Text();
    while (totalBytesToSample > 0) {
      if (System.currentTimeMillis() - lastTime > 1000 * 60) {
        lastTime = System.currentTimeMillis();
        long sampledBytes = totalSampleSize - totalBytesToSample;
        LOG.info("Sampled " + (100 * sampledBytes / totalSampleSize) + "%");
      }

      long randomFilePosition = (Math.abs(random.nextLong()) % inTotalSize);
      int fileIndex = 0;
      while (randomFilePosition > inputSize[fileIndex]) {
        randomFilePosition -= inputSize[fileIndex];
        fileIndex++;
      }
      if (inputSize[fileIndex] - randomFilePosition < MaxLineLength) {
        randomFilePosition -= MaxLineLength;
      }
      
      inputStreams[fileIndex].seek(randomFilePosition);
      byte lastReadByte;
      do {
        lastReadByte = inputStreams[fileIndex].readByte();
      } while (lastReadByte != '\n' && lastReadByte != '\r');

      byte readLine[] = new byte[MaxLineLength];

      int readLineIndex = 0;

      while (inputStreams[fileIndex].getPos() < inputSize[fileIndex] - 1) {
        lastReadByte = inputStreams[fileIndex].readByte();
        if (lastReadByte == '\n' || lastReadByte == '\r') {
          break;
        }

        readLine[readLineIndex++] = lastReadByte;
      }
      // Skip an empty line
      if (readLineIndex < 4)
        continue;
      text.set(readLine, 0, readLineIndex);
      shape.fromText(text);

      samplePoints.add(shape.getMBR().getCenterPoint());
      totalBytesToSample -= readLineIndex;
    }
    
    for (int fileIndex = 0; fileIndex < inputPaths.length; fileIndex++) {
      inputStreams[fileIndex].close();
    }
    
    Point[] samples = samplePoints.toArray(new Point[samplePoints.size()]);
    LOG.info("Picked a sample of size: "+samples.length);
    return samples;
  }

}
