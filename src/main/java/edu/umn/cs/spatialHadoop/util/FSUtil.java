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
   * Flatten the given directory by removing one lever from the directory
   * hierarchy. It finds all subdirectories under the given directory and merges
   * them together while putting their contents into the given directory.
   * It is assumed that the given path does not have any files, only subdirs.
   * @param path
   */
  public static void flattenDirectory(FileSystem fs, Path path) throws IOException {
    // Decide which directory to use as a destination directory based on the
    // number of files in each one
    FileStatus[] subdirs = fs.listStatus(path);
    int maxSize = 0;
    Path destinationPath = null;
    for (FileStatus subdir : subdirs) {
      if (subdir.isDirectory()) {
        int size = fs.listStatus(subdir.getPath()).length;
        System.out.println("subdir="+subdir+"size="+size);
        if (size > maxSize) {
          maxSize = size;
          destinationPath = subdir.getPath();
        }
      }
    }
    System.out.println("Destination Path depending on size of subdirs:"+destinationPath);
    // Scan the paths again and move their contents to the destination path
    for (FileStatus subdir : subdirs) {
      System.out.println("Subdir="+subdir);
      if (subdir.isDirectory() && subdir.getPath() != destinationPath) {
        // Scan all the contents of this path and move it to the destination path
        FileStatus[] files = fs.listStatus(subdir.getPath());
        for (FileStatus file : files) {
          fs.rename(file.getPath(), new Path(destinationPath, file.getPath().getName()));
        }
        // Now, since the path is empty, we can safely delete it
        // We delete it with non-recursive option for safety
        fs.delete(subdir.getPath(), true);
        
      }
    }

    // Finally, rename the destination directory to make it similar to its parent
    Path parentPath = path;
    System.out.println("parentPath="+parentPath);
    Path renamedParent = new Path(parentPath.getParent(), Math.random()+".tmp");
    System.out.println("RenamedParent="+renamedParent);
    fs.rename(parentPath, renamedParent);
    // Destination path has now changed since we renamed its parent
    destinationPath = new Path(renamedParent, destinationPath.getName());
    System.out.println("DestinationPath="+destinationPath);
    fs.rename(destinationPath, parentPath);
    System.out.println("DestinationPath="+destinationPath);
    fs.delete(renamedParent, false);

  }

}
