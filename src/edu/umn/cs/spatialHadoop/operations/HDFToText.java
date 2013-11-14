package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.NASADataset;
import edu.umn.cs.spatialHadoop.core.NASAPoint;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat3;
import edu.umn.cs.spatialHadoop.mapred.HDFInputFormat;

/**
 * This operation transforms one or more HDF files into text files which can
 * be used with other operations. Each point in the HDF file will be represented
 * as one line in the text output.
 * @author Ahmed Eldawy
 *
 */
public class HDFToText {
  
  public static class HDFToTextMap extends MapReduceBase implements
      Mapper<NASADataset, NASAPoint, Rectangle, NASAPoint> {

    public void map(NASADataset dataset, NASAPoint point,
        OutputCollector<Rectangle, NASAPoint> output, Reporter reporter)
            throws IOException {
      output.collect(dataset.mbr, point);
    }
  }
  
  /**
   * Performs an HDF to text operation as a MapReduce job and returns total
   * number of points generated.
   * @param inPath
   * @param outPath
   * @param datasetName
   * @param skipFillValue
   * @return
   * @throws IOException
   */
  public static long HDFToTextMapReduce(Path inPath, Path outPath,
      String datasetName, boolean skipFillValue) throws IOException {
    JobConf job = new JobConf(HDFToText.class);
    job.setJobName("HDFToText");

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();

    // Set Map function details
    job.setMapperClass(HDFToTextMap.class);
    job.setMapOutputKeyClass(Rectangle.class);
    job.setMapOutputValueClass(NASAPoint.class);
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    // Set input information
    job.setInputFormat(HDFInputFormat.class);
    HDFInputFormat.setInputPaths(job, inPath);
    job.set(HDFInputFormat.DatasetName, datasetName);
    job.setBoolean(HDFInputFormat.SkipFillValue, skipFillValue);
    
    // Set output information
    job.setOutputFormat(GridOutputFormat3.class);
    GridOutputFormat3.setOutputPath(job, outPath);
    
    // Run the job
    RunningJob lastSubmittedJob = JobClient.runJob(job);
    Counters counters = lastSubmittedJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    return resultCount;
  }

  private static void printUsage() {
    System.out.println("Converts a set of HDF files to text format");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("<output file>: (*) Path to output file");
    System.out.println("dataset:<dataset>: (*) Name of the dataset to read from HDF");
    System.out.println("-skipfillvalue: Skip fill value");
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(HDFToText.class);
    Path[] paths = cla.getPaths();
    if (paths.length < 2) {
      printUsage();
      System.err.println("Please provide both input and output files");
      return;
    }
    Path inPath = paths[0];
    Path outPath = paths[1];
    
    FileSystem fs = inPath.getFileSystem(conf);
    if (!fs.exists(inPath)) {
      printUsage();
      System.err.println("Input file does not exist");
      return;
    }
    
    String datasetName = cla.get("dataset");
    boolean skipFillValue = cla.is("skipfillvalue");

    HDFToTextMapReduce(inPath, outPath, datasetName, skipFillValue);
  }
}
