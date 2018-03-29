package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.operations.OperationMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Appends new data to an existing index
 */
@OperationMetadata(shortName="append",
description = "Appends a data file to an existing index")
public class IndexAppender {

  public static void append(Path inPath, Path indexPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    FileSystem fs = indexPath.getFileSystem(params);
    if (!fs.exists(indexPath)) {
      // A new index, create it
      Indexer.index(inPath, indexPath, params);
    } else {
      // An existing index, append to it
      Path tempPath;
      do {
        tempPath = new Path(indexPath.getParent(), Integer.toString((int) (Math.random()*1000000)));
      } while (fs.exists(tempPath));
      try {
        // Index the input in reference to the existing index
        Indexer.repartition(inPath, tempPath, indexPath, params);

        // Concatenate corresponding files
        Map<Integer, Partition> mergedIndex = new HashMap<Integer, Partition>();
        for (Partition p : SpatialSite.getGlobalIndex(fs, indexPath)) {
          mergedIndex.put(p.cellId, p);
        }
        for (Partition newP : SpatialSite.getGlobalIndex(fs, tempPath)) {
          Partition existingP = mergedIndex.get(newP.cellId);
          // Combine the partition information
          existingP.expand(newP);
          // Combine the file
          Path pathOfExisting = new Path(indexPath, existingP.filename);
          Path pathOfNew = new Path(tempPath, newP.filename);
          concat(params, fs, pathOfExisting, pathOfNew);
        }
        // Write back the merge partitions as a new global index
        Path masterFilePath = fs.listStatus(indexPath, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.getName().startsWith("_master");
          }
        })[0].getPath();
        FSDataOutputStream out = fs.create(masterFilePath, true);
        Text line = new Text();
        for (Partition p : mergedIndex.values()) {
          line.clear();
          p.toText(line);
          TextSerializerHelper.appendNewLine(line);
          out.write(line.getBytes(), 0, line.getLength());
        }
        out.close();

        Partitioner.generateMasterWKT(fs, masterFilePath);
      } finally {
        fs.delete(tempPath, true);
      }
    }
  }

  /**
   * Concatenates a set of files (in the same file system) into one file
   * @param dest
   * @param src
   */
  protected static void concat(Configuration conf, FileSystem fs, Path dest, Path ... src) throws IOException {
    try {
      // Try a possibly efficient implementation provided by the FileSystem
      fs.concat(dest, src);
    } catch (UnsupportedOperationException e) {
      Path tempPath = null;
      // Unsupported by the file system, provide a less efficient naive implementation
      OutputStream out;
      if (!fs.exists(dest)) {
        // Destination file does not exist. Create it
        out = fs.create(dest);
      } else {
        // Destination file exists, try appending to it
        try {
          out = fs.append(dest);
        } catch (IOException ee) {
          // Append not supported, create a new file and write the existing destFile first
          do {
            tempPath = new Path(dest.getParent(), Integer.toString((int) (Math.random() * 1000000)));
          } while (fs.exists(tempPath));
          out = fs.create(tempPath);
          InputStream in = fs.open(dest);
          IOUtils.copyBytes(in, out, conf, false);
          in.close();
        }
      }
      for (Path s : src) {
        if (!s.equals(dest)) {
          InputStream in = fs.open(s);
          IOUtils.copyBytes(in, out, conf, false);
          in.close();
        }
      }
      out.close();
      if (tempPath != null) {
        fs.delete(dest, true);
        fs.rename(tempPath, dest);
      }
    }
  }

  protected static void printUsage() {
    System.out.println("Adds more data to an existing index");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file that contains the new data");
    System.out.println("<index path> - (*) Path to the index");
    System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
    System.out.println("sindex:<index> - Type of spatial index (grid|str|str+|rtree|r+tree|quadtree|zcurve|hilbert|kdtree)");
    System.out.println("gindex:<index> - Type of the global index (grid|str|rstree|kdtree|zcurve|hilbert|quadtree)");
    System.out.println("lindex:<index> - Type of the local index (rrstree)");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }


  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

    if (!params.checkInput()) {
      printUsage();
      return;
    }
    Path inputPath = params.getInputPath();
    Path outputPath = params.getOutputPath();

    // The spatial index to use
    long t1 = System.nanoTime();
    append(inputPath, outputPath, params);
    long t2 = System.nanoTime();
    System.out.printf("Total append time %f seconds\n",(t2-t1)*1E-9);

  }
}
