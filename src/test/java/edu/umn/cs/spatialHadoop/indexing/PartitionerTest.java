package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.PrintStream;

public class PartitionerTest extends TestCase {

  public void testGenerateMasterWKT() {
    try {
      Path masterPath = new Path("_master.rstar");
      FileSystem fs = masterPath.getFileSystem(new Configuration());
      PrintStream ps = new PrintStream(fs.create(masterPath, true));
      Partition p = new Partition("data000", new CellInfo(1, 0,5,100,105));
      ps.println(p.toText(new Text()));
      ps.close();

      Partitioner.generateMasterWKT(fs, masterPath);
      Path wktPath = new Path("_rstar.wkt");
      assertTrue("WKT file not found!", fs.exists(wktPath));

    } catch (IOException e) {
      e.printStackTrace();
      fail("Error in test!");
    }

  }
}