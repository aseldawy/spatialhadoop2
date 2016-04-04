/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.mapred.BinaryRecordReader;
import edu.umn.cs.spatialHadoop.mapred.BinarySpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.RTreeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayRecordReader;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionMap;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionMapNoReplication;
import edu.umn.cs.spatialHadoop.util.Progressable;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * 
 * @author Ahmed Eldawy
 *
 */
public class DistributedJoin {
	private static final Log LOG = LogFactory.getLog(DistributedJoin.class);
	public static RunningJob lastRunningJob;
	public static int maxBytesInOneRead = 1024 * 1024 * 100;
	public static int maxShapesInOneRead = 2000;
	public static boolean isOneShotReadMode = true;
	public static boolean isGeneralRepartitionMode = true;
	public static boolean isReduceInactive = false;
	public static boolean isFilterOnly = false;
	public static int joiningThresholdPerOnce = 50000;

	private static final String RepartitionJoinIndexPath = "DJ.RepartitionJoinIndexPath";
	private static final String InactiveMode = "DJ.InactiveMode";
	private static final String isFilterOnlyMode = "DJ.FilterOnlyMode";
	private static final String JoiningThresholdPerOnce = "DJ.JoiningThresholdPerOnce";
	
	public static class SpatialJoinFilter extends DefaultBlockFilter {
		@Override
		public void selectCellPairs(GlobalIndex<Partition> gIndex1,
				GlobalIndex<Partition> gIndex2,
				final ResultCollector2<Partition, Partition> output) {
			// Do a spatial join between the two global indexes
			GlobalIndex.spatialJoin(gIndex1, gIndex2,
					new ResultCollector2<Partition, Partition>() {
						@Override
						public void collect(Partition r, Partition s) {
							Rectangle intersection = r.getIntersection(s);
							if (intersection != null
									&& intersection.getWidth()
											* intersection.getHeight() > 0) {
								output.collect(r, s);
							} else {
								LOG.info("Skipping touching partitions " + r
										+ ", " + s);
							}
						}
					});
		}
	}

	static class SelfJoinMap extends MapReduceBase implements
			Mapper<Rectangle, ArrayWritable, Shape, Shape> {
		@Override
		public void map(Rectangle key, ArrayWritable value,
				final OutputCollector<Shape, Shape> output, Reporter reporter)
				throws IOException {
			Shape[] objects = (Shape[]) value.get();
			SpatialAlgorithms.SelfJoin_planeSweep(objects, true, output,
			    new Progressable.ReporterProgressable(reporter));
		}
	}

	public static class RedistributeJoinMap extends MapReduceBase
			implements
			Mapper<PairWritable<Rectangle>, PairWritable<? extends Writable>, Shape, Shape> {

		private boolean isFilterOnly;
		
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			isFilterOnly = OperationsParams.getFilterOnlyModeFlag(job, isFilterOnlyMode);
		}
		
		public void map(final PairWritable<Rectangle> key,
				final PairWritable<? extends Writable> value,
				final OutputCollector<Shape, Shape> output, final Reporter reporter)
				throws IOException {
		
		  final Rectangle dupAvoidanceMBR = !key.first.isValid()
					&& !key.second.isValid() ? null // Both blocks are heap blocks
					: (!key.first.isValid() ? key.second // Second block is indexed
							: (!key.second.isValid() ? key.first // First block is indexed
									: (key.first.getIntersection(key.second)))); // Both indexed

		  final Rectangle mapperMBR = dupAvoidanceMBR == null? null:
		    dupAvoidanceMBR.buffer(dupAvoidanceMBR.getWidth()/1000, dupAvoidanceMBR.getHeight()/1000);

			if (value.first instanceof ArrayWritable
					&& value.second instanceof ArrayWritable) {
				// Join two arrays using the plane sweep algorithm
				if (mapperMBR != null) {
					// Only join shapes in the intersection rectangle
					List<Shape> r = new Vector<Shape>();
					List<Shape> s = new Vector<Shape>();
					for (Shape shape : (Shape[]) ((ArrayWritable) value.first).get()) {
						Rectangle mbr = shape.getMBR();
						if (mbr != null && mapperMBR.isIntersected(mbr))
							r.add(shape);
					}
					for (Shape shape : (Shape[]) ((ArrayWritable) value.second).get()) {
						Rectangle mbr = shape.getMBR();
						if (mbr != null && mapperMBR.isIntersected(mbr))
							s.add(shape);
					}
					
					if(isFilterOnly){
						SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
									  try {
									    double intersectionX = Math.max(
									        r.getMBR().x1, s.getMBR().x1);
									    double intersectionY = Math.max(
									        r.getMBR().y1, s.getMBR().y1);
									    // Employ reference point duplicate avoidance technique
                      if (dupAvoidanceMBR.contains(intersectionX, intersectionY))
									      output.collect(r, s);
									  } catch (IOException e) {
									    e.printStackTrace();
									  }	
									}
								}, reporter);	
					} else {
						SpatialAlgorithms.SpatialJoin_planeSweep(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
									  try {
									    double intersectionX = Math.max(
									        r.getMBR().x1, s.getMBR().x1);
									    double intersectionY = Math.max(
									        r.getMBR().y1, s.getMBR().y1);
									    // Employ reference point duplicate avoidance technique
                      if (dupAvoidanceMBR.contains(intersectionX, intersectionY))
									      output.collect(r, s);
									  } catch (IOException e) {
									    e.printStackTrace();
									  }	
									}
								}, reporter);	
					}
				} else {

					ArrayList<Shape> r = new ArrayList<Shape>();
					ArrayList<Shape> s = new ArrayList<Shape>();
					// Copy non-empty records
					for (Shape shape : (Shape[]) ((ArrayWritable) value.first)
							.get()) {
						if (shape.getMBR() != null)
							r.add(shape);
					}
					for (Shape shape : (Shape[]) ((ArrayWritable) value.second)
							.get()) {
						if (shape.getMBR() != null)
							s.add(shape);
					}

					if(isFilterOnly){
						SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
										try {
											output.collect(r, s);
										} catch (IOException e) {
											e.printStackTrace();
										}
									}
								}, reporter);
					}else{
						SpatialAlgorithms.SpatialJoin_planeSweep(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
										try {
											output.collect(r, s);
										} catch (IOException e) {
											e.printStackTrace();
										}
									}
						}, reporter);
					}
				}
			} else if (value.first instanceof RTree
					&& value.second instanceof RTree) {
				// Join two R-trees
				@SuppressWarnings("unchecked")
				RTree<Shape> r1 = (RTree<Shape>) value.first;
				@SuppressWarnings("unchecked")
				RTree<Shape> r2 = (RTree<Shape>) value.second;
				RTree.spatialJoin(r1, r2, new ResultCollector2<Shape, Shape>() {
					@Override
					public void collect(Shape r, Shape s) {
						try {
							if (dupAvoidanceMBR == null) {
								output.collect(r, s);
							} else {
								// Reference point duplicate avoidance technique
								// The reference point is the lowest corner of
								// the intersection
								// rectangle (the point with the least
								// dimensions of both x and
								// y in the intersection rectangle)
								double intersectionX = Math.max(r.getMBR().x1, s.getMBR().x1);
								double intersectionY = Math.max(r.getMBR().y1, s.getMBR().y1);
                if (dupAvoidanceMBR.contains(intersectionX, intersectionY))
									output.collect(r, s);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}, reporter);
			} else {
				throw new RuntimeException("Cannot join "
						+ value.first.getClass() + " with "
						+ value.second.getClass());
			}
			reporter.progress();
		}
	}

	public static class RedistributeJoinMapNoDupAvoidance extends MapReduceBase
			implements
			Mapper<PairWritable<Rectangle>, PairWritable<? extends Writable>, Shape, Shape> {

		private boolean isFilterOnly;
		
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			isFilterOnly = OperationsParams.getFilterOnlyModeFlag(job, isFilterOnlyMode);
		}
		
		public void map(final PairWritable<Rectangle> key,
				final PairWritable<? extends Writable> value,
				final OutputCollector<Shape, Shape> output, final Reporter reporter)
				throws IOException {
			
			final Rectangle mapperMBR = !key.first.isValid()
					&& !key.second.isValid() ? null // Both blocks are heap blocks
					: (!key.first.isValid() ? key.second // Second block is indexed
							: (!key.second.isValid() ? key.first // First block is indexed
									: (key.first.getIntersection(key.second)))); // Both indexed
			// We add a small buffer to account for points which are very close to the edge
	     if (mapperMBR != null)
	       mapperMBR.set(mapperMBR.buffer(mapperMBR.getWidth()/1000, mapperMBR.getHeight()/1000));

			if (value.first instanceof ArrayWritable
					&& value.second instanceof ArrayWritable) {
				// Join two arrays using the plane sweep algorithm
				if (mapperMBR != null) {
					// Only join shapes in the intersection rectangle
					ArrayList<Shape> r = new ArrayList<Shape>();
					ArrayList<Shape> s = new ArrayList<Shape>();
					for (Shape shape : (Shape[]) ((ArrayWritable) value.first)
							.get()) {
						Rectangle mbr = shape.getMBR();
						if (mbr != null && mapperMBR.isIntersected(mbr))
							r.add(shape);
					}
					for (Shape shape : (Shape[]) ((ArrayWritable) value.second)
							.get()) {
						Rectangle mbr = shape.getMBR();
						if (mbr != null && mapperMBR.isIntersected(mbr))
							s.add(shape);
					}
					
					if(isFilterOnly){
						SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
									  try {
									    output.collect(r, s);
									  } catch (IOException e) {
									    e.printStackTrace();
									  }	
									}
								}, reporter);	
					}else{
						SpatialAlgorithms.SpatialJoin_planeSweep(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
									  try {
									    output.collect(r, s);
									  } catch (IOException e) {
									    e.printStackTrace();
									  }	
									}
								}, reporter);	
					}
				} else {
					ArrayList<Shape> r = new ArrayList<Shape>();
					ArrayList<Shape> s = new ArrayList<Shape>();
					// Copy non-empty records
					for (Shape shape : (Shape[]) ((ArrayWritable) value.first)
							.get()) {
						if (shape.getMBR() != null)
							r.add(shape);
					}
					for (Shape shape : (Shape[]) ((ArrayWritable) value.second)
							.get()) {
						if (shape.getMBR() != null)
							s.add(shape);
					}

					if(isFilterOnly){
						SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
										try {
											output.collect(r, s);
										} catch (IOException e) {
											e.printStackTrace();
										}	
									}
								}, reporter);	
					}else{
						SpatialAlgorithms.SpatialJoin_planeSweep(r, s,
								new ResultCollector2<Shape, Shape>() {
									@Override
									public void collect(Shape r, Shape s) {
										try {
											output.collect(r, s);
										} catch (IOException e) {
											e.printStackTrace();
										}	
									}
								}, reporter);
					}
				}
			} else if (value.first instanceof RTree
					&& value.second instanceof RTree) {
				// Join two R-trees
				@SuppressWarnings("unchecked")
				RTree<Shape> r1 = (RTree<Shape>) value.first;
				@SuppressWarnings("unchecked")
				RTree<Shape> r2 = (RTree<Shape>) value.second;
				RTree.spatialJoin(r1, r2, new ResultCollector2<Shape, Shape>() {
					@Override
					public void collect(Shape r, Shape s) {
						try {
							output.collect(r, s);
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}, reporter);
			} else {
				throw new RuntimeException("Cannot join "
						+ value.first.getClass() + " with "
						+ value.second.getClass());
			}
			reporter.progress();
		}
	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 * 
	 * @author Ahmed Eldawy
	 *
	 */
	public static class DJInputFormatArray extends
			BinarySpatialInputFormat<Rectangle, ArrayWritable> {

		/**
		 * Reads a pair of arrays of shapes
		 * 
		 * @author Ahmed Eldawy
		 *
		 */
		public static class DJRecordReader extends
				BinaryRecordReader<Rectangle, ArrayWritable> {
			public DJRecordReader(Configuration conf,
					CombineFileSplit fileSplits) throws IOException {
				super(conf, fileSplits);
			}

			@Override
			protected RecordReader<Rectangle, ArrayWritable> createRecordReader(
					Configuration conf, CombineFileSplit split, int i)
					throws IOException {
				FileSplit fsplit = new FileSplit(split.getPath(i),
						split.getStartOffsets()[i], split.getLength(i),
						split.getLocations());
				return new ShapeArrayRecordReader(conf, fsplit);
			}
		}

		@Override
		public RecordReader<PairWritable<Rectangle>, PairWritable<ArrayWritable>> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			reporter.progress();
			return new DJRecordReader(job, (CombineFileSplit) split);
		}
	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 * 
	 * @author Ahmed Eldawy
	 *
	 */
	public static class DJInputFormatRTree<S extends Shape> extends
			BinarySpatialInputFormat<Rectangle, RTree<S>> {

		/**
		 * Reads a pair of arrays of shapes
		 * 
		 * @author Ahmed Eldawy
		 *
		 */
		public static class DJRecordReader<S extends Shape> extends
				BinaryRecordReader<Rectangle, RTree<S>> {
			public DJRecordReader(Configuration conf,
					CombineFileSplit fileSplits) throws IOException {
				super(conf, fileSplits);
			}

			@Override
			protected RecordReader<Rectangle, RTree<S>> createRecordReader(
					Configuration conf, CombineFileSplit split, int i)
					throws IOException {
				FileSplit fsplit = new FileSplit(split.getPath(i),
						split.getStartOffsets()[i], split.getLength(i),
						split.getLocations());
				return new RTreeRecordReader<S>(conf, fsplit);
			}
		}

		@Override
		public RecordReader<PairWritable<Rectangle>, PairWritable<RTree<S>>> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			reporter.progress();
			return new DJRecordReader<S>(job, (CombineFileSplit) split);
		}
	}

	/**
   * Select a file to repartition based on some heuristics. If only one file is
   * indexed, the non-indexed file is repartitioned. If both files are indexed,
   * the smaller file is repartitioned.
   * 
   * @param files
   * @param params
   * @return the index in the given array of the file to be repartitioned. -1 if
   *         all files are non-indexed
   * @throws IOException
   */
	protected static int selectRepartition(final Path[] files,
			OperationsParams params) throws IOException {
		int largest_partitioned_file = -1;
		long largest_size = 0;

		for (int i_file = 0; i_file < files.length; i_file++) {
			FileSystem fs = files[i_file].getFileSystem(params);
			GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs,
					files[i_file]);
			if (gindex != null) {
				// Compute total size (all files in directory)
				long total_size = 0;
				for (Partition p : gindex) {
					Path file = new Path(files[i_file], p.filename);
					total_size += fs.getFileStatus(file).getLen();
				}
				if (total_size > largest_size) {
					largest_partitioned_file = i_file;
					largest_size = total_size;
				}
			}
		}
		return largest_partitioned_file == -1 ? -1
				: 1 - largest_partitioned_file;
	}

	/**
	 * Repartition a file to match the partitioning of the other file.
	 * @param files Input files to partition
	 * @param file_to_repartition
	 * @param params
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static void repartitionStep(final Path[] files,
			int file_to_repartition, OperationsParams params)
			throws IOException, InterruptedException {

		// Do the repartition step
		long t1 = System.currentTimeMillis();

		// Repartition the smaller file
		Path partitioned_file;
		FileSystem fs = files[file_to_repartition].getFileSystem(params);
		do {
			partitioned_file = new Path(files[file_to_repartition].getName()
					+ ".repartitioned_" + (int) (Math.random() * 1000000));
		} while (fs.exists(partitioned_file));

		// Get the cells to use for repartitioning
		GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs,
				files[1 - file_to_repartition]);
		CellInfo[] cells = SpatialSite.cellsOf(fs,
				files[1 - file_to_repartition]);

		// Repartition the file to match the other file
		boolean isReplicated = gindex.isReplicated();
		boolean isCompact = gindex.isCompact();
		String sindex;
		if (isReplicated && !isCompact)
			sindex = "grid";
		else if (isReplicated && isCompact)
			sindex = "r+tree";
		else if (!isReplicated && isCompact)
			sindex = "rtree";
		else
			throw new RuntimeException("Unknown index at: "
					+ files[1 - file_to_repartition]);

		params.set("sindex", sindex);

		if (isGeneralRepartitionMode) {
			// Repartition the smaller file with heuristics cells info (general
			// indexing)
			Repartition.repartitionMapReduce(files[file_to_repartition],
					partitioned_file, null, params);
		} else {
			// Repartition the smaller file on the larger file (specific
			// indexing)
			Repartition.repartitionMapReduce(files[file_to_repartition],
					partitioned_file, cells, params);
		}

		long t2 = System.currentTimeMillis();
		System.out.println("Repartition time " + (t2 - t1) + " millis");

		// Continue with the join step
		if (fs.exists(partitioned_file)) {
			// An output file might not existent if the two files are disjoint

			// Replace the smaller file with its repartitioned copy
			files[file_to_repartition] = partitioned_file;

			// Delete temporary repartitioned file upon exit
			fs.deleteOnExit(partitioned_file);
		}
	}

	/**
	 * Performs a redistribute join between the given files using the
   * redistribute join algorithm. Currently, we only support a pair of files.
	 * @param inFiles
	 * @param userOutputPath
	 * @param params
	 * @return
	 * @throws IOException
	 */
	public static <S extends Shape> long joinStep(Path[] inFiles,
			Path userOutputPath, OperationsParams params) throws IOException {
		long t1 = System.currentTimeMillis();

		JobConf job = new JobConf(params, DistributedJoin.class);

		FileSystem fs[] = new FileSystem[inFiles.length];
		for (int i_file = 0; i_file < inFiles.length; i_file++)
			fs[i_file] = inFiles[i_file].getFileSystem(job);

		Path outputPath = userOutputPath;
		if (outputPath == null) {
			do {
				outputPath = new Path(inFiles[0].getName() + ".dj_"
						+ (int) (Math.random() * 1000000));
			} while (fs[0].exists(outputPath));
		}

		job.setJobName("DistributedJoin");
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		GlobalIndex<Partition> gindex1 = SpatialSite.getGlobalIndex(fs[0],
				inFiles[0]);
		GlobalIndex<Partition> gindex2 = SpatialSite.getGlobalIndex(fs[1],
				inFiles[1]);

		OperationsParams.setFilterOnlyModeFlag(job, isFilterOnlyMode, isFilterOnly);
		
		LOG.info("Joining " + inFiles[0] + " X " + inFiles[1]);

		if (SpatialSite.isRTree(fs[0], inFiles[0])
				&& SpatialSite.isRTree(fs[1], inFiles[1])) {
			job.setInputFormat(DJInputFormatRTree.class);
		} else {
			if (isOneShotReadMode) {
				// Ensure all objects are read in one shot
				job.setInt(SpatialSite.MaxBytesInOneRead, -1);
				job.setInt(SpatialSite.MaxShapesInOneRead, -1);
			} else {
				job.setInt(SpatialSite.MaxBytesInOneRead, maxBytesInOneRead);
				job.setInt(SpatialSite.MaxShapesInOneRead, maxShapesInOneRead);
			}
			job.setInputFormat(DJInputFormatArray.class);
		}

		// Set input paths and map function
		if (inFiles[0].equals(inFiles[1])) {
			// Self join
			job.setInputFormat(ShapeArrayInputFormat.class);
			// Remove the spatial filter to ensure all partitions are loaded
			FileInputFormat.setInputPaths(job, inFiles[0]);
			if (gindex1 != null && gindex1.isReplicated())
				job.setMapperClass(RedistributeJoinMap.class);
			else
				job.setMapperClass(RedistributeJoinMapNoDupAvoidance.class);
		} else {
			// Binary version of spatial join (two different input files)
			job.setClass(SpatialSite.FilterClass, SpatialJoinFilter.class,
					BlockFilter.class);
			FileInputFormat.setInputPaths(job, inFiles);
			if ((gindex1 != null && gindex1.isReplicated())
					|| (gindex2 != null && gindex2.isReplicated())) {
				// Need the map function with duplicate avoidance step.
				job.setMapperClass(RedistributeJoinMap.class);
			} else {
				// No replication in both indexes, use map function with no dup
				// avoidance
				job.setMapperClass(RedistributeJoinMapNoDupAvoidance.class);
			}
		}

		Shape shape = params.getShape("shape");
		job.setMapOutputKeyClass(shape.getClass());
		job.setMapOutputValueClass(shape.getClass());
		job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
		job.setNumReduceTasks(0); // No reduce needed for this task

		if (job.getBoolean("output", true))
		  job.setOutputFormat(TextOutputFormat.class);
		else
		  job.setOutputFormat(NullOutputFormat.class);

		TextOutputFormat.setOutputPath(job, outputPath);

		if (!params.getBoolean("background", false)) {
			LOG.info("Submit job in sync mode");
			RunningJob runningJob = JobClient.runJob(job);
			Counters counters = runningJob.getCounters();
			Counter outputRecordCounter = counters
					.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
			final long resultCount = outputRecordCounter.getValue();

			// Output number of running map tasks
			Counter mapTaskCountCounter = counters
					.findCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
			System.out.println("Number of map tasks "
					+ mapTaskCountCounter.getValue());

			// Delete output directory if not explicitly set by user
			if (userOutputPath == null)
				fs[0].delete(outputPath, true);
			long t2 = System.currentTimeMillis();
			System.out.println("Join time " + (t2 - t1) + " millis");

			return resultCount;
		} else {
			JobClient jc = new JobClient(job);
			LOG.info("Submit job in async mode");
			lastRunningJob = jc.submitJob(job);
			LOG.info("Job " + lastRunningJob + " submitted successfully");
			return -1;
		}
	}

	public static class RepartitionJoinReduce<T extends Shape> extends
			MapReduceBase implements Reducer<IntWritable, T, Shape, Shape> {

		private Path indexDir;
		private Shape shape;
		private boolean inactiveMode;
		private boolean isFilterOnly;
		private int shapesThresholdPerOnce;
		
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			indexDir = OperationsParams.getRepartitionJoinIndexPath(job,
					RepartitionJoinIndexPath);
			shape = OperationsParams.getShape(job, "shape");
			inactiveMode = OperationsParams.getInactiveModeFlag(job, InactiveMode);
			isFilterOnly = OperationsParams.getFilterOnlyModeFlag(job, isFilterOnlyMode);
			shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
		}

		@Override
		public void reduce(IntWritable cellIndex, Iterator<T> shapes,
				final OutputCollector<Shape, Shape> output, Reporter reporter)
				throws IOException {
		 if(!inactiveMode){
			
			LOG.info("Start reduce() logic now !!!");
			final FileSystem fs = indexDir.getFileSystem(new Configuration());
			GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(fs,
					indexDir);
			
			
			// Get collected shapes from the repartition phase
			while (shapes.hasNext()) {
					int currRShapes = 0;
					final ArrayList<Shape> r = new ArrayList<Shape>();
					do{
						T rShape = shapes.next();
						r.add(rShape.clone());	
						currRShapes++;
					} while(shapes.hasNext() && currRShapes < shapesThresholdPerOnce);

					
					for (Partition p : gIndex) {
						if (p.cellId == cellIndex.get()) {
							LOG.info("Joining with partition: "+p);
							Path partitionFile = new Path(indexDir, p.filename);
							FileSystem partitionFS = partitionFile
									.getFileSystem(new Configuration());

							// Load all shapes in this partition
							ShapeIterRecordReader shapeReader = new ShapeIterRecordReader(
									partitionFS.open(partitionFile), 0, p.size);
							shapeReader.setShape(shape);
							Rectangle cellInfo = shapeReader.createKey();
							ShapeIterator partitionShapes = shapeReader
									.createValue();

							// load shapes from the indexed dataset
							while (shapeReader.next(cellInfo, partitionShapes)) {
								while(partitionShapes.hasNext()){
									final ArrayList<Shape> selectedSShapes = new ArrayList<Shape>();
									int currSShapes = 0;
									do{
										Shape shapeInPartition = partitionShapes.next();
										selectedSShapes.add(shapeInPartition.clone());
										currSShapes++;
									} while(partitionShapes.hasNext() && currSShapes < shapesThresholdPerOnce);
									LOG.info("Read "+selectedSShapes.size()+" shapes from partition");
									
									// Join two arrays using the plane sweep
									// algorithm
									if(isFilterOnly){
										SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(
												r, selectedSShapes,
												new ResultCollector2<Shape, Shape>() {
													@Override
													public void collect(Shape r, Shape s) {
													  try {
													    output.collect(r, s);
													  } catch (IOException e) {
													    e.printStackTrace();
													  }	
													}
												}, reporter);
									}else{
										SpatialAlgorithms.SpatialJoin_planeSweep(
												r, selectedSShapes,
												new ResultCollector2<Shape, Shape>() {
													@Override
													public void collect(Shape r, Shape s) {
													  try {
													    output.collect(r, s);
													  } catch (IOException e) {
													    e.printStackTrace();
													  }	
													}
												}, reporter);										
									}
									
			
									reporter.progress();	
								}
							}	
							shapeReader.close();
							

							LOG.info("Finished joining of "+p);
						}
					}	
			}
			
					
		}else{
			LOG.info("Nothing to do !!!");
		}
	  }
	}

	/**
	 * Spatially joins two datasets by repartitioning the smaller dataset based
	 * on the larger one, then apply one-to-one joining for each partition
	 * 
	 * @author Ibrahim Sabek
	 * @param inputFiles
	 *            Input datasets to be spatially joined
	 * @param fileToRepartition
	 *            Index of which file will be repartitioned
	 * @param outputFile
	 *            Output file contains the joining results
	 * @param params
	 *            Job configurations
	 * @return
	 * @throws IOException
	 */
	protected static long repartitionJoinStep(final Path[] inputFiles,
			int fileToRepartition, Path outputFile, OperationsParams params)
			throws IOException {

		boolean overwrite = params.getBoolean("overwrite", false);
		Shape stockShape = params.getShape("shape");

		// Do the repartition step
		long t1 = System.currentTimeMillis();

		JobConf repartitionJoinJob = new JobConf(params, DistributedJoin.class);
		repartitionJoinJob.setJobName("RepartitionJoin");

		FileSystem fs = inputFiles[fileToRepartition].getFileSystem(params);

		Path outputPath = outputFile;
		if (outputPath == null) {
			do {
				outputPath = new Path(inputFiles[0].getName() + ".dj_"
						+ (int) (Math.random() * 1000000));
			} while (fs.exists(outputPath));
		}

		LOG.info("Repartition - Joining " + inputFiles[0] + " X "
				+ inputFiles[1]);

		// Get the cells to use for repartitioning
		GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs,
				inputFiles[1 - fileToRepartition]);
		OperationsParams.setRepartitionJoinIndexPath(repartitionJoinJob,
				RepartitionJoinIndexPath, inputFiles[1 - fileToRepartition]);
		OperationsParams.setInactiveModeFlag(repartitionJoinJob, InactiveMode, isReduceInactive);
		OperationsParams.setJoiningThresholdPerOnce(repartitionJoinJob, JoiningThresholdPerOnce, joiningThresholdPerOnce);
		OperationsParams.setFilterOnlyModeFlag(repartitionJoinJob, isFilterOnlyMode, isFilterOnly);
		CellInfo[] cellsInfo = SpatialSite.cellsOf(fs,
				inputFiles[1 - fileToRepartition]);

		// Repartition the file to match the other file
		boolean isReplicated = gindex.isReplicated();
		boolean isCompact = gindex.isCompact();
		String sindex;
		if (isReplicated && !isCompact)
			sindex = "grid";
		else if (isReplicated && isCompact)
			sindex = "r+tree";
		else if (!isReplicated && isCompact)
			sindex = "rtree";
		else
			throw new RuntimeException("Unknown index at: "
					+ inputFiles[1 - fileToRepartition]);
		params.set("sindex", sindex);

		// Decide which map function to use based on the type of global index
		if (sindex.equals("rtree") || sindex.equals("str")) {
			// Repartition without replication
			repartitionJoinJob
					.setMapperClass(RepartitionMapNoReplication.class);
		} else {
			// Repartition with replication (grid and r+tree)
			repartitionJoinJob.setMapperClass(RepartitionMap.class);
		}
		repartitionJoinJob.setMapOutputKeyClass(IntWritable.class);
		repartitionJoinJob.setMapOutputValueClass(stockShape.getClass());
		ShapeInputFormat.setInputPaths(repartitionJoinJob,
				inputFiles[fileToRepartition]);
		repartitionJoinJob.setInputFormat(ShapeInputFormat.class);

		ClusterStatus clusterStatus = new JobClient(repartitionJoinJob)
				.getClusterStatus();
		repartitionJoinJob.setNumMapTasks(10 * Math.max(1,
				clusterStatus.getMaxMapTasks()));

		SpatialSite.setCells(repartitionJoinJob, cellsInfo);
		repartitionJoinJob.setBoolean(SpatialSite.OVERWRITE, overwrite);

		// set reduce function
		repartitionJoinJob.setReducerClass(RepartitionJoinReduce.class);
		repartitionJoinJob.setNumReduceTasks(Math.max(1, Math.min(
				cellsInfo.length,
				(clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

		repartitionJoinJob.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(repartitionJoinJob, outputPath);

		RunningJob runningJob = JobClient.runJob(repartitionJoinJob);
		Counters counters = runningJob.getCounters();
		Counter outputRecordCounter = counters
				.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
		final long resultCount = outputRecordCounter.getValue();

		// Output number of running map tasks
		Counter mapTaskCountCounter = counters
				.findCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
		System.out.println("Number of map tasks "
				+ mapTaskCountCounter.getValue());

		// Delete output directory if not explicitly set by user
		if (outputFile == null)
			fs.delete(outputPath, true);
		long t2 = System.currentTimeMillis();
		System.out.println("Repartitioning and Joining time " + (t2 - t1)
				+ " millis");

		return resultCount;
	}

	/**
	 * Spatially joins two files.
	 * @param inputFiles
	 * @param userOutputPath
	 * @param params
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	public static long distributedJoinSmart(final Path[] inputFiles,
			Path userOutputPath, OperationsParams params) throws IOException, InterruptedException {
		Path[] originalInputFiles = inputFiles.clone();
		FileSystem outFs = inputFiles[0].getFileSystem(params);
		Path outputPath = userOutputPath;
		if (outputPath == null) {
			do {
				outputPath = new Path(inputFiles[0].getName() + ".dj_"
						+ (int) (Math.random() * 1000000));
			} while (outFs.exists(outputPath));
		}

		// Decide whether to do a repartition step or not
		int cost_with_repartition, cost_without_repartition;
		final FileStatus[] fStatus = new FileStatus[inputFiles.length];
		for (int i_file = 0; i_file < inputFiles.length; i_file++) {
			// TODO work with folders. Calculate size more accurately
			FileSystem fs = inputFiles[i_file].getFileSystem(params);
			fStatus[i_file] = fs.getFileStatus(inputFiles[i_file]);
		}

		// Sort files by length (size)
		IndexedSortable filesBySize = new IndexedSortable() {
			@Override
			public void swap(int i, int j) {
				Path tmp1 = inputFiles[i];
				inputFiles[i] = inputFiles[j];
				inputFiles[j] = tmp1;

				FileStatus tmp2 = fStatus[i];
				fStatus[i] = fStatus[j];
				fStatus[j] = tmp2;
			}

			@Override
			public int compare(int i, int j) {
				if (fStatus[i].getLen() < fStatus[j].getLen())
					return 0;
				return fStatus[i].getLen() < fStatus[j].getLen() ? -1 : 1;
			}
		};

		new QuickSort().sort(filesBySize, 0, inputFiles.length);
		GlobalIndex<Partition>[] gIndexes = new GlobalIndex[fStatus.length];
		int[] numBlocks = new int[fStatus.length];
		for (int i_file = 0; i_file < fStatus.length; i_file++) {
			gIndexes[i_file] = SpatialSite.getGlobalIndex(outFs,
					fStatus[i_file].getPath());
			if (gIndexes[i_file] != null) {
				// Number of blocks is equal to number of partitions in global
				// index
				numBlocks[i_file] = gIndexes[i_file].size();
			} else if (fStatus[i_file].isDir()) {
				// Add up number of file system blocks in all subfiles of this
				// directory
				numBlocks[i_file] = 0;
				FileStatus[] subfiles = outFs.listStatus(inputFiles[i_file],
						SpatialSite.NonHiddenFileFilter);
				for (FileStatus subfile : subfiles) {
					numBlocks[i_file] += outFs.getFileBlockLocations(subfile,
							0, subfile.getLen()).length;
				}
			} else {
				// Number of file system blocks in input file
				numBlocks[i_file] = outFs.getFileBlockLocations(
						fStatus[i_file], 0, fStatus[i_file].getLen()).length;
			}
		}

		cost_without_repartition = gIndexes[0] != null && gIndexes[1] != null ? GlobalIndex
				.spatialJoin(gIndexes[0], gIndexes[1], null)
				: (numBlocks[0] * numBlocks[1]);
		// Total cost = Cost of repartition (=== 2 * numBlocks[0]) +
		// cost of join (=== numBlocks[0] + numBlocks[1])
		cost_with_repartition = numBlocks[0] * 3 + numBlocks[1];
		LOG.info("Cost with repartition is estimated to "
				+ cost_with_repartition);
		LOG.info("Cost without repartition is estimated to "
				+ cost_without_repartition);
		boolean need_repartition = cost_with_repartition < cost_without_repartition;
		if (need_repartition) {
			int file_to_repartition = selectRepartition(inputFiles, params);
			repartitionStep(inputFiles, file_to_repartition, params);
		}

		// Restore inputFiles to the original order by user
		if (inputFiles[1] != originalInputFiles[1]) {
			Path temp = inputFiles[0];
			inputFiles[0] = inputFiles[1];
			inputFiles[1] = temp;
		}

		// Redistribute join the larger file and the partitioned file
		long result_size = DistributedJoin.joinStep(inputFiles, outputPath,
				params);

		if (userOutputPath == null)
			outFs.delete(outputPath, true);

		return result_size;
	}

	private static long selfJoinLocal(Path in, Path out, OperationsParams params)
			throws IOException {
		if (isOneShotReadMode) {
			// Ensure all objects are read in one shot
			params.setInt(SpatialSite.MaxBytesInOneRead, -1);
			params.setInt(SpatialSite.MaxShapesInOneRead, -1);
		} else {
			params.setInt(SpatialSite.MaxBytesInOneRead, maxBytesInOneRead);
			params.setInt(SpatialSite.MaxShapesInOneRead, maxShapesInOneRead);
		}
		ShapeArrayInputFormat inputFormat = new ShapeArrayInputFormat();
		JobConf job = new JobConf(params);
		FileInputFormat.addInputPath(job, in);
		InputSplit[] splits = inputFormat.getSplits(job, 1);
		FileSystem outFs = out.getFileSystem(params);
		final PrintStream writer = new PrintStream(outFs.create(out));

		// Process all input files
		long resultSize = 0;
		for (InputSplit split : splits) {
			ShapeArrayRecordReader reader = new ShapeArrayRecordReader(job,
					(FileSplit) split);
			final Text temp = new Text();

			Rectangle key = reader.createKey();
			ArrayWritable value = reader.createValue();
			if (reader.next(key, value)) {
				Shape[] writables = (Shape[]) value.get();
				resultSize += SpatialAlgorithms.SelfJoin_planeSweep(writables,
						true, new OutputCollector<Shape, Shape>() {
							@Override
							public void collect(Shape r, Shape s)
									throws IOException {
								writer.print(r.toText(temp));
								writer.print(",");
								writer.println(s.toText(temp));
							}
						}, null);
				if (reader.next(key, value)) {
					throw new RuntimeException(
							"Error! Not all values read in one shot.");
				}
			}

			reader.close();
		}
		writer.close();

		return resultSize;
	}

	private static void printUsage() {
		System.out
				.println("Performs a spatial join between two files using the distributed join algorithm");
		System.out.println("Parameters: (* marks the required parameters)");
		System.out.println("<input file 1> - (*) Path to the first input file");
		System.out
				.println("<input file 2> - (*) Path to the second input file");
		System.out.println("<output file> - Path to output file");
		System.out
				.println("repartition:<decision> - (*) Decision to repartition smaller dataset (yes|no|auto)");
		System.out
				.println("all-inmemory-load:<decision> - (*) Decision to load all file blocks in memory (yes|no)");
		System.out
				.println("heuristic-repartition:<decision> - (*) Decision to have a heuristic or exact repartition (yes|no)");
		System.out
				.println("direct-join:<decision> - (*) Decision to directly join after repartitioning (yes|no)");
		System.out.println("-overwrite - Overwrite output file without notice");

		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args));
		Path[] allFiles = params.getPaths();
		if (allFiles.length < 2) {
			System.err
					.println("This operation requires at least two input files");
			printUsage();
			System.exit(1);
		}
		if (allFiles.length == 2 && !params.checkInput()) {
			// One of the input files does not exist
			printUsage();
			System.exit(1);
		}
		if (allFiles.length > 2 && !params.checkInputOutput()) {
			printUsage();
			System.exit(1);
		}

		Path[] inputPaths = allFiles.length == 2 ? allFiles : params.getInputPaths();
		Path outputPath = allFiles.length == 2 ? null : params.getOutputPath();

		if (params.get("heuristic-repartition", "yes").equals("no")) {
			isGeneralRepartitionMode = false;
			System.out.println("heuristic-repartition is false");
		}

		if (params.get("all-inmemory-load", "yes").equals("no")) {
			isOneShotReadMode = false;
			System.out.println("all-inmemory-load is false");
		}

		if (params.get("direct-join", "no").equals("yes")) {
			System.out
					.println("Reparition the smaller dataset then join the two datasets directly");
		}
		
		if (params.get("repartition-only", "no").equals("yes")) {
			System.out.println("Repartition-only is true");
			isReduceInactive = true;
		}

		if (params.get("joining-per-once") != null) {
			System.out.println("joining-per-once is set to: " + params.get("joining-per-once"));
			joiningThresholdPerOnce = Integer.parseInt(params.get("joining-per-once"));
		}
		
		if (params.get("filter-only") != null) {
			System.out.println("filer-only mode is set to: " + params.get("filter-only"));
			if (params.get("filter-only").equals("yes")) {
				isFilterOnly = true;
			}else{
				isFilterOnly = false;
			}
		}
		
		long result_size;
		if (inputPaths[0].equals(inputPaths[1])) {
			// Special case for self join
			selfJoinLocal(inputPaths[0], outputPath, params);
		}
		
		String repartition = params.get("repartition", "no");
		if (repartition.equals("auto")) {
			result_size = distributedJoinSmart(inputPaths, outputPath, params);
		} else if (repartition.equals("yes")) {
			int file_to_repartition = selectRepartition(inputPaths, params);
			if (params.get("direct-join").equals("yes")) {
				result_size = repartitionJoinStep(inputPaths,
						file_to_repartition, outputPath, params);
			} else {
				repartitionStep(inputPaths, file_to_repartition, params);
				result_size = joinStep(inputPaths, outputPath, params);
			}
		} else if (repartition.equals("no")) {
			result_size = joinStep(inputPaths, outputPath, params);
		} else {
			throw new RuntimeException("Illegal parameter repartition:"
					+ repartition);
		}

		System.out.println("Result size: " + result_size);
	}
}
