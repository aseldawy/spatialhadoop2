package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.operations.OperationMetadata;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialHadoop.util.MetadataUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Appends new data to an existing index
 */
@OperationMetadata(shortName="append",
description = "Appends a data file to an existing index")
public class IndexAppender {

  public static void append(Path inPath, Path indexPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    append(new Path[] {inPath}, indexPath, params);
  }

  public static void append(Path[] inPath, Path indexPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
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
          FileUtil.concat(params, fs, pathOfExisting, pathOfNew);
        }
        // Write back the merged partitions as a new global index
        Path masterFilePath = fs.listStatus(indexPath, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.getName().startsWith("_master");
          }
        })[0].getPath();
        writeMasterFile(fs, masterFilePath, mergedIndex.values());

        Partitioner.generateMasterWKT(fs, masterFilePath);
      } finally {
        fs.delete(tempPath, true);
      }
    }
  }

  private static void writeMasterFile(FileSystem fs, Path masterFilePath,
                                      Collection<Partition> partitions) throws IOException {
    FSDataOutputStream out = fs.create(masterFilePath, true);
    Text line = new Text();
    for (Partition p : partitions) {
      line.clear();
      p.toText(line);
      TextSerializerHelper.appendNewLine(line);
      out.write(line.getBytes(), 0, line.getLength());
    }
    out.close();
  }

  public static void reorganize(Path indexPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    List<List<Partition>> splitGroups = RTreeOptimizer.getSplitGroups(indexPath, params, RTreeOptimizer.OptimizerType.MaximumReducedCost);
    FileSystem fs = indexPath.getFileSystem(params);
    // A list of temporary paths where the reorganized partitions will be stored.
    Path[] tempPaths = new Path[splitGroups.size()];
    Job[] indexJobs = new Job[splitGroups.size()];
    for (int iGroup = 0; iGroup < splitGroups.size(); iGroup++) {
      List<Partition> group = splitGroups.get(iGroup);
      OperationsParams indexParams = new OperationsParams(params);
      indexParams.setBoolean("background", true);
      indexParams.setBoolean("local", false);
      Path[] inPaths = new Path[group.size()];
      for (int iPartition = 0; iPartition < group.size(); iPartition++) {
        inPaths[iPartition] = new Path(indexPath, group.get(iPartition).filename);
      }
      do {
        tempPaths[iGroup] = new Path(indexPath.getParent(), Integer.toString((int) Math.random() * 1000000));
      } while (fs.exists(tempPaths[iGroup]));
      indexJobs[iGroup] = Indexer.index(inPaths, tempPaths[iGroup], indexParams);
    }

    // A list of all the new partitions (after reorganization)
    List<Partition> mergedPartitions = MetadataUtil.getPartitions(indexPath, params);
    int maxId = Integer.MIN_VALUE;
    for (Partition p : mergedPartitions)
      maxId = Math.max(maxId, p.cellId);
    // Wait until all index jobs are done
    for (int iGroup = 0; iGroup < indexJobs.length; iGroup++) {
      Job job = indexJobs[iGroup];
      job.waitForCompletion(false);
      if (!job.isSuccessful())
        throw new RuntimeException("Job " + job + " was unsuccessful");
      // Remove all partitions that were indexed by the job
      for (Partition oldP : splitGroups.get(iGroup)) {
        if (!mergedPartitions.remove(oldP))
          throw new RuntimeException("The partition " + oldP + " is being reorganized but does not exist in " + indexPath);
      }

      ArrayList<Partition> newPartitions = MetadataUtil.getPartitions(tempPaths[iGroup], params);
      for (Partition newPartition : newPartitions) {
        // Generate a new ID and filename to ensure it does not override an existing file
        newPartition.cellId = ++maxId;
        String newName = String.format("part-%05d", newPartition.cellId);
        // TODO Copy the extension of the existing file if it exists
        fs.rename(new Path(tempPaths[iGroup], newPartition.filename), new Path(indexPath, newName));
        newPartition.filename = newName;
        mergedPartitions.add(newPartition);
      }
    }
    // Write back the partitions to the master file
    Path masterPath = fs.listStatus(indexPath, SpatialSite.MasterFileFilter)[0].getPath();
    writeMasterFile(fs, masterPath, mergedPartitions);

    // Delete partitions that have been reorganized and replaced
    for (List<Partition> group : splitGroups) {
      for (Partition partition : group) {
        fs.delete(new Path(indexPath, partition.filename), false);
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
    Path newDataPath = params.getInputPath();
    Path existingIndexPath = params.getOutputPath();

    // The spatial index to use
    long t1 = System.nanoTime();
    addToIndex(newDataPath, existingIndexPath, params);
    long t2 = System.nanoTime();
    System.out.printf("Total append time %f seconds\n",(t2-t1)*1E-9);

  }

  public static void addToIndex(Path newDataPath, Path existingIndexPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    append(newDataPath, existingIndexPath, params);
    reorganize(existingIndexPath, params);
  }
}
