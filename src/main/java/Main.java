import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    // Join two input files
    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput())
      throw new RuntimeException("Input-output combination not correct");
    
    Path[] inputs = params.getInputPaths();
    
    Path output = params.getOutputPath();
    FileSystem fsout = output.getFileSystem(params);
    final PrintWriter pwout = new PrintWriter(fsout.create(output));
    
    ResultCollector2<Shape, Shape> results = new ResultCollector2<Shape, Shape>() {
      @Override
      public void collect(Shape r, Shape s) {
        pwout.println(r+";"+s);
      }
    };
    
    if (params.getBoolean("rtree", false)) {
      long t1 = System.currentTimeMillis();
      // Join two input R-trees
      Shape shape = params.getShape("shape");
      RTree<Shape>[] rtrees = new RTree[inputs.length];
      for (int i = 0; i < inputs.length; i++) {
        FileSystem fs = inputs[i].getFileSystem(params);
        rtrees[i] = new RTree<Shape>();
        rtrees[i].setStockObject(shape.clone());
        FSDataInputStream in = fs.open(inputs[i]);
        rtrees[i].readFields(in);
        rtrees[i].toWKT(System.out);
      }
      int resultSize = RTree.spatialJoin(rtrees[0], rtrees[1], results, null);
      long t2 = System.currentTimeMillis();
      System.out.println("Found "+resultSize+" pairs in "+(t2-t1)/1000.0+" seconds");
    } else {
      long t1 = System.currentTimeMillis();
      // Join two input raw files
      List<Shape>[] shapes = new List[inputs.length];
      for (int i = 0; i < inputs.length; i++) {
        FileSystem fs = inputs[i].getFileSystem(params);
        long length = fs.getFileStatus(inputs[i]).getLen();
        FSDataInputStream in = fs.open(inputs[i]);
        SpatialRecordReader3<Shape> reader = new SpatialRecordReader3<Shape>();
        reader.initialize(new FileSplit(inputs[i], 0, length, new String[0]), params);
        shapes[i] = new ArrayList<Shape>();
        while (reader.nextKeyValue()) {
          Iterable<Shape> shapeIter = reader.getCurrentValue();
          for (Shape s : shapeIter) {
            shapes[i].add(s.clone());
          }
        }
        in.close();
      }
      
      // Join the two input files
      int resultSize = SpatialAlgorithms.SpatialJoin_planeSweep(shapes[0], shapes[1], results, null);
      long t2 = System.currentTimeMillis();
      System.out.println("Found "+resultSize+" pairs in "+(t2-t1)/1000.0+" seconds");
    }
    
    pwout.close();
  }

}
