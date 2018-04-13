package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.util.MetadataUtil;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.List;

public class CellPartitionerTest extends BaseTest {

  public void testOverlapPartition() {
    Path masterPath = new Path("src/test/resources/test.cells");
    Path indexPath = new Path(scratchPath, "tempindex");
    try {
      Configuration conf = new Configuration();
      FileUtil.copy(masterPath.getFileSystem(conf), masterPath,
          indexPath.getFileSystem(conf), new Path(indexPath, "_master.rstar"),
          false, conf);
      //List<Partition> partitions = MetadataUtil.getPartitions(masterPath);
      CellInfo[] cells = SpatialSite.cellsOf(masterPath.getFileSystem(conf), indexPath);
      CellPartitioner p = new CellPartitioner(cells);
      int cellId = p.overlapPartition(new Point(-124.7306754,40.4658126));
      assertEquals(42, cellId);
    } catch (IOException e) {
      throw new RuntimeException("Error in test!", e);
    }
  }

  public void testCanBeInitializedWithPartitions() {
    Partition[] partitions = {
      new Partition("data1", new CellInfo(1, 0, 5, 10, 20)),
      new Partition("data2", new CellInfo(2, 0, 5, 10, 20)),
    };
    try {

      CellPartitioner partitioner = new CellPartitioner(partitions);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      partitioner.write(dos);
      dos.close();

      byte[] bytes = baos.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais);
      partitioner = new CellPartitioner();
      partitioner.readFields(dis);

      assertEquals(2, partitioner.cells.length);
      assertEquals(2, partitioner.cells[1].cellId);
    } catch (IOException e) {
      throw new RuntimeException("Error in test", e);
    }

    Path masterPath = new Path("src/test/resources/test.cells");
    Path indexPath = new Path(scratchPath, "tempindex");
    try {
      Configuration conf = new Configuration();
      FileUtil.copy(masterPath.getFileSystem(conf), masterPath,
          indexPath.getFileSystem(conf), new Path(indexPath, "_master.rstar"),
          false, conf);
      //List<Partition> partitions = MetadataUtil.getPartitions(masterPath);
      CellInfo[] cells = SpatialSite.cellsOf(masterPath.getFileSystem(conf), indexPath);
      CellPartitioner p = new CellPartitioner(cells);
      int cellId = p.overlapPartition(new Point(-124.7306754,40.4658126));
      assertEquals(42, cellId);
    } catch (IOException e) {
      throw new RuntimeException("Error in test!", e);
    }
  }
}