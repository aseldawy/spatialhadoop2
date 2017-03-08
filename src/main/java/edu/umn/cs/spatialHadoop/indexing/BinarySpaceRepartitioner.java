package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.DynamicRepartitioner.DynamicRepartitionerMap;
import edu.umn.cs.spatialHadoop.indexing.DynamicRepartitioner.DynamicRepartitionerReduce;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

public class BinarySpaceRepartitioner {
	
	private static final Log LOG = LogFactory.getLog(Indexer.class);
	
	private static Job repartitionMapReduce(Path inPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(params, "BinarySpaceRepartitioner");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DynamicRepartitioner.class);
		
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		String sindex = params.get("sindex");
		double overflowSize = blockSize * overflowRate;

		// Get list of data files
		FileSystem inFs = inPath.getFileSystem(conf);
		FileStatus[] resultFiles = inFs.listStatus(inPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().contains("part-");
			}
		});

		if (resultFiles.length == 0) {
			LOG.warn("Input data is empty.");
			return null;
		} else {
			ArrayList<Partition> partitionsToSplit = new ArrayList<Partition>();
			ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
			FilePartitioner partitioner = new FilePartitioner();
			int maxCellId = 0;
			
			// Read master file to get the list of overflow partitions
			Path masterPath = new Path(inPath, "_master." + sindex);
			Text tempLine = new Text2();
			LineReader in = new LineReader(inFs.open(masterPath));
			while (in.readLine(tempLine) > 0) {
				Partition tempPartition = new Partition();
				tempPartition.fromText(tempLine);
				currentPartitions.add(tempPartition);
				
				if (maxCellId < tempPartition.cellId) {
					maxCellId = tempPartition.cellId;
				}
			}
			
			partitionsToSplit.clear();
			partitioner.cells.clear();
			for(Partition p: currentPartitions) {
				if(p.size > overflowSize) {
					partitionsToSplit.add(p);
					Rectangle[] rects = p.cellMBR.split();
					CellInfo[] splitCells = new CellInfo[2];
					for(int i = 0; i < splitCells.length; i++) {
						maxCellId += 1;
						splitCells[i] = new CellInfo();
						splitCells[i].set(rects[i]);
						splitCells[i].cellId = maxCellId;
						partitioner.cells.add(splitCells[i]);
					}
				}
			}
			System.out.println("number of cells in filer partitioner = " + partitioner.cells.size());
			for(CellInfo cell: partitioner.cells) {
				System.out.println(cell.toString());
			}
			
			if(partitionsToSplit.size() > 0) {
				for(Partition p: partitionsToSplit) {
					System.out.println("partition to split: " + p.filename);
				}
				Partitioner.setPartitioner(conf, partitioner);
				
				Path[] splitFiles = new Path[partitionsToSplit.size()];
				for (int i = 0; i < partitionsToSplit.size(); i++) {
					splitFiles[i] = new Path(inPath, partitionsToSplit.get(i).filename);
					inFs.deleteOnExit(splitFiles[i]);
				}
				
				// Set mapper and reducer
				Shape shape = OperationsParams.getShape(conf, "shape");
				job.setMapperClass(DynamicRepartitionerMap.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(shape.getClass());
				job.setReducerClass(DynamicRepartitionerReduce.class);
				// Set input and output
				job.setInputFormatClass(SpatialInputFormat3.class);
				SpatialInputFormat3.setInputPaths(job, splitFiles);
				job.setOutputFormatClass(IndexOutputFormat.class);
				Path tempPath = new Path(inPath, "temp");
				IndexOutputFormat.setOutputPath(job, tempPath);
				// Set number of reduce tasks according to cluster status
				ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
				job.setNumReduceTasks(Math.max(1,
						Math.min(partitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));

				// Use multithreading in case the job is running locally
				conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

				// Start the job
				if (conf.getBoolean("background", false)) {
					// Run in background
					job.submit();
				} else {
					job.waitForCompletion(conf.getBoolean("verbose", false));
				}
			} else {
				System.out.println("There is no file that need to be splitted.");
				return null;
			}
		}

		return job;
	}
	
	private static void printUsage() {
		System.out.println("Binary repartition indexed files");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<sindex> - (*) Index type of input file");
		System.out.println("<overflow_rate> - (*) Overflow rate of repartition operation");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 1)) {
			printUsage();
			System.exit(1);
		}

		long t1 = System.currentTimeMillis();
		Path inPath = inputFiles[0];
		System.out.println("Input path: " + inPath);
		Job job;
		do {
			job = repartitionMapReduce(inPath, params);
			if(job != null) {
				DynamicRepartitioner.mergeFiles(inPath, params, job);
			}
		} while(job != null);
		long t2 = System.currentTimeMillis();
	    System.out.println("Total repartitioning time in millis "+(t2-t1));
	}
}
