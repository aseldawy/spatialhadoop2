package edu.umn.cs.spatialHadoop;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.RandomSpatialGenerator;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class TestHelper {

  public static void generateFile(String filename, Class<? extends Shape> shapeClass,
                                  Rectangle mbr, long size, OperationsParams params) throws InterruptedException, IOException, ClassNotFoundException {
    params.setClass("shape", Point.class, Shape.class);
    OperationsParams.setShape(params, "mbr", mbr);
    params.setBoolean("local", true);
    params.setBoolean("overwrite", true);
    params.setLong("size", size);
    Path randomFile = new Path("TestHelper.RandomFile");
    RandomSpatialGenerator.generateFile(randomFile, params);
    File[] generatedFiles = new File(randomFile.getName()).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        char firstChar = name.charAt(0);
        return firstChar != '.' && firstChar != '_';
      }
    });
    if (generatedFiles.length == 1) {
      generatedFiles[0].renameTo(new File(filename));
    } else {
      new File(randomFile.getName()).renameTo(new File(filename));
    }
    randomFile.getFileSystem(params).deleteOnExit(randomFile);
  }
}
