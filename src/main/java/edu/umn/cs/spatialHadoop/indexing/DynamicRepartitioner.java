package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class DynamicRepartitioner {
	
	private static final Log LOG = LogFactory.getLog(Indexer.class);
	
	/**
	 * The map function that partitions the data using the configured
	 * partitioner. Refer from Indexer class
	 * 
	 * @author Tin Vu
	 *
	 */
	public static class DynamicRepartitionerMap extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private Partitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to
		 * assign it to only one partition
		 */
		private boolean replicate;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.partitioner = Partitioner.getPartitioner(context.getConfiguration());
			this.replicate = context.getConfiguration().getBoolean("replicate", false);
		}

		@Override
		protected void map(Rectangle key, Iterable<? extends Shape> shapes, final Context context)
				throws IOException, InterruptedException {
			final IntWritable partitionID = new IntWritable();
			for (final Shape shape : shapes) {
				if (replicate) {
					partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
						@Override
						public void collect(Integer r) {
							partitionID.set(r);
							try {
								context.write(partitionID, shape);
							} catch (IOException e) {
								LOG.warn("Error checking overlapping partitions", e);
							} catch (InterruptedException e) {
								LOG.warn("Error checking overlapping partitions", e);
							}
						}
					});
				} else {
					partitionID.set(partitioner.overlapPartition(shape));
					if (partitionID.get() >= 0)
						context.write(partitionID, shape);
				}
				context.progress();
			}
		}
	}

	public static class DynamicRepartitionerReduce<S extends Shape> extends Reducer<IntWritable, Shape, IntWritable, Shape> {

		@Override
		protected void reduce(IntWritable partitionID, Iterable<Shape> shapes, Context context)
				throws IOException, InterruptedException {
			LOG.info("Working on partition #" + partitionID);
			for (Shape shape : shapes) {
				context.write(partitionID, shape);
				context.progress();
			}
			// Indicate end of partition to close the file
			context.write(new IntWritable(-partitionID.get() - 1), null);
			LOG.info("Done with partition #" + partitionID);
		}
	}

	private static Job repartitionMapReduce(Path inPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(params, "DynamicRepartitioner");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DynamicRepartitioner.class);

		// Get list of data files
		FileSystem inFs = inPath.getFileSystem(conf);
		FileStatus[] resultFiles = inFs.listStatus(inPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().contains("part-");
			}
		});
		
		if(resultFiles.length == 0) {
			LOG.warn("Input data is empty.");
		} else {
			List<Path> inFileList = new ArrayList<Path>();
			for (FileStatus f : resultFiles) {
				inFileList.add(f.getPath());
			}
			
			Path[] inFiles = inFileList.toArray(new Path[inFileList.size()]);
			
			// Set input file MBR if not already set
			Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
			if (inputMBR == null) {
				inputMBR = FileMBR.fileMBR(inFiles, new OperationsParams(conf));
				OperationsParams.setShape(conf, "mbr", inputMBR);
			}

			// Set the correct partitioner according to index type
			String index = conf.get("sindex");
			if (index == null)
				throw new RuntimeException("Index type is not set");

			long t1 = System.currentTimeMillis();
			// setLocalIndexer(conf, index);
			Path tempPath = new Path(inPath, "temp");
			Partitioner partitioner = Indexer.createPartitioner(inFiles, tempPath, conf, index);
			Partitioner.setPartitioner(conf, partitioner);

			long t2 = System.currentTimeMillis();
			System.out.println("Total time for space subdivision in millis: " + (t2 - t1));
			
			ArrayList<PotentialPartition> partitionsToSplit = getPartitionsToSplit(inPath, partitioner, params);
			
			// Split partition
			for(PotentialPartition pp: partitionsToSplit) {
				FilePartitioner filePartitioner = new FilePartitioner();
				for(IntersectionInfo intersection: pp.intersections) {
					Partition tempPartition = new Partition();
					tempPartition.set(intersection.getCell());
					filePartitioner.cells.add(tempPartition);
				}
				Partitioner.setPartitioner(conf, filePartitioner);
				
				// Set mapper and reducer
			    Shape shape = OperationsParams.getShape(conf, "shape");
			    job.setMapperClass(DynamicRepartitionerMap.class);
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(shape.getClass());
			    job.setReducerClass(DynamicRepartitionerReduce.class);
			    // Set input and output
			    job.setInputFormatClass(SpatialInputFormat3.class);
			    SpatialInputFormat3.setInputPaths(job, new Path(inPath, pp.filename));
			    job.setOutputFormatClass(IndexOutputFormat.class);
				IndexOutputFormat.setOutputPath(job, tempPath);
			    // Set number of reduce tasks according to cluster status
			    ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
			    job.setNumReduceTasks(Math.max(1, Math.min(partitioner.getPartitionCount(),
			        (clusterStatus.getMaxReduceTasks() * 9) / 10)));

			    // Use multithreading in case the job is running locally
			    conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
			    
			    // Start the job
			    if (conf.getBoolean("background", false)) {
			      // Run in background
			      job.submit();
			    } else {
			      job.waitForCompletion(conf.getBoolean("verbose", false));
			    }
			}
		}

		return job;
	}

	private static ArrayList<PotentialPartition> getPartitionsToSplit(Path inPath, final Partitioner partitioner, OperationsParams params) throws IOException {
		final ArrayList<PotentialPartition> potentialPartitions = new ArrayList<PotentialPartition>();
		ArrayList<PotentialPartition> partitionsToSplit = new ArrayList<PotentialPartition>();
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");
		String jsim = params.get("jsim");
		double jsimValue = Double.parseDouble(jsim);

		// Load current partitions
		Path masterPath = new Path(inPath, "_master." + sindex);
		Text tempLine = new Text2();
		LineReader in = new LineReader(fs.open(masterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			currentPartitions.add(tempPartition);
		}
		in.close();

		// Sampling to get current standard partitions
		for(final Partition p: currentPartitions) {
			partitioner.overlapPartitions(p, new ResultCollector<Integer>() {
				@Override
				public void collect(Integer r) {
					CellInfo overlappedCell = partitioner.getPartition(r);
					// Compute Jaccard Similarity between current partition and overlapped partition
					Rectangle intersectionArea = p.getIntersection(overlappedCell);
					double unionAreaSize = p.getSize() + overlappedCell.getSize() - intersectionArea.getSize();
					double jsValue = intersectionArea.getSize() / unionAreaSize;
					PotentialPartition potentialPartition = new PotentialPartition(p);
					potentialPartition.intersections.add(new IntersectionInfo(overlappedCell, jsValue));
					potentialPartitions.add(potentialPartition);
				}
			});
		}
		
		// Iterate the list of potential partitions to get the partitions to split
		for(PotentialPartition pp: potentialPartitions) {
			boolean keep = false;
			for(IntersectionInfo intersection: pp.intersections) {
				if(intersection.getJsValue() >= jsimValue) {
					keep = true;
				}
			}
			if(!keep) {
				partitionsToSplit.add(pp);
			}
		}
		
		return partitionsToSplit;
	}

	private static void printUsage() {
		System.out.println("Dynamic repartition indexed files with low cost");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 1)) {
			printUsage();
			System.exit(1);
		}

		Path inPath = inputFiles[0];
		System.out.println("Input path: " + inPath);
		repartitionMapReduce(inPath, params);
	}

}
