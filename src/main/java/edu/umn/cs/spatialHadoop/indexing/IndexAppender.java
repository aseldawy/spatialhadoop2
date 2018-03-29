package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Appends new data to an existing index
 */
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
      // Index the input in reference to the existing index
      Indexer.repartition(inPath, tempPath, indexPath, params);
      fs.deleteOnExit(tempPath);
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
        p.toText(line);
        out.write(line.getBytes(), 0, line.getLength());
      }
      out.close();
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
}
