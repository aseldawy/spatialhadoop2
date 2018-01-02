package edu.umn.cs.spatialHadoop.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Some file system utiliy functions.
 * Created by Ahmed Eldawy on 1/1/18.
 */
public class FSUtil {

  /**
   * Merges several paths into their parent path. Simply, it removes one level
   * from the file system hierarchy by moving all files in the given paths
   * one level up. This function can be used to merge the output of several
   * MapReduce jobs into one directory.
   * @param paths
   */
  public static void mergeAndFlattenPaths(FileSystem fs, Path ... paths) throws IOException {
    // Decide which directory to use as a destination directory based on the
    // number of files in each one
    int maxSize = 0;
    Path destinationPath = null;
    for (Path path : paths) {
      if (path != null) {
        int size = fs.listStatus(path).length;
        if (size > maxSize) {
          maxSize = size;
          destinationPath = path;
        }
      }
    }

    // Scan the paths again and move their contents to the destination path
    for (Path path : paths) {
      if (path != null && path != destinationPath) {
        // Scan all the contents of this path and move it to the destination path
        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
          fs.rename(file.getPath(), new Path(destinationPath, file.getPath().getName()));
        }
        // Now, since the path is empty, we can safely delete it
        // We delete it with non-recursive option for safety
        fs.delete(path, false);
      }
    }

    // Finally, rename the destination directory to make it similar to its parent
    Path parentPath = destinationPath.getParent();
    Path renamedParent = new Path(parentPath.getParent(), Math.random()+".tmp");
    fs.rename(parentPath, renamedParent);
    // Destination path has now changed since we renamed its parent
    destinationPath = new Path(renamedParent, destinationPath.getName());
    fs.rename(destinationPath, parentPath);
    fs.delete(renamedParent, false);

  }

}
