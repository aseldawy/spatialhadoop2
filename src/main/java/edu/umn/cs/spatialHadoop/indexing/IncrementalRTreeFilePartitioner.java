package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.LineReader;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.EntryDefault;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

public class IncrementalRTreeFilePartitioner extends Partitioner {

	private static final double MINIMUM_EXPANSION = Double.MAX_VALUE;
	private static final int MAXIMUM_POINTS = 10000;
	// private RTree<Integer, Geometry> tree;
	public ArrayList<Partition> cells = new ArrayList<Partition>();
	private RTree<Integer, Geometry> cellsTree;
	private final Object lock = new Object();

	private RTree<Integer, Geometry> buildCellsTree(ArrayList<Partition> cellList) {
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();
		for (CellInfo cell : cellList) {
			Rectangle r = cell.getMBR();
			entries.add(new EntryDefault<Integer, Geometry>(cell.cellId, Geometries.rectangle(r.x1, r.y1, r.x2, r.y2)));
		}

		RTree<Integer, Geometry> tree = RTree.star().maxChildren(10).create();
		tree = tree.add(entries);

		return tree;
	}

	private List<Partition> getNearestCells(Shape shape, int maxCount) {
//		System.out.println("Getting nearest cells");
		ArrayList<Integer> nearestCellIds = new ArrayList<>();

		Rectangle r = shape.getMBR();
		List<Entry<Integer, Geometry>> entries = this.cellsTree
				.nearest(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2), 50, maxCount).toList().toBlocking().single();
//		List<Entry<Integer, Geometry>> entries = this.cellsTree.search(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2))
//				.toList().toBlocking().single();
		for (Entry<Integer, Geometry> entry : entries) {
			Integer cellId = entry.value();
			nearestCellIds.add(cellId);
		}
		List<Partition> nearestCells = this.cells.stream().filter(c -> nearestCellIds.contains(new Integer(c.cellId)))
				.collect(Collectors.toList());

		return nearestCells;
		// return null;
	}
	
	private List<Partition> getOverlappingCells(Shape shape) {
		ArrayList<Integer> overlappingCellIds = new ArrayList<>();
		Rectangle r = shape.getMBR();
		
		List<Entry<Integer, Geometry>> entries = this.cellsTree.search(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2))
				.toList().toBlocking().single();
		for (Entry<Integer, Geometry> entry : entries) {
			Integer cellId = entry.value();
			overlappingCellIds.add(cellId);
		}
		List<Partition> overlappingCells = this.cells.stream().filter(c -> overlappingCellIds.contains(new Integer(c.cellId)))
				.collect(Collectors.toList());

		return overlappingCells;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cells.size());
		for (Partition cell : this.cells) {
			cell.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int cellsSize = in.readInt();
		cells = new ArrayList<Partition>(cellsSize);
		for (int i = 0; i < cellsSize; i++) {
			Partition cellInfo = new Partition();
			cellInfo.readFields(in);
			cells.add(cellInfo);
		}
		cellsTree = this.buildCellsTree(cells);
	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int capacity) throws IllegalArgumentException {
		// TODO Auto-generated method stub

	}

	public void createFromInputFile(Path inPath, Path out, OperationsParams params) throws IOException {
		ArrayList<Partition> currentPartitions = new ArrayList<Partition>();

		Job job = new Job(params, "Sampler");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		double blockSize = Double.parseDouble(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");

		Path currentMasterPath = new Path(inPath, "_master." + sindex);
		Text tempLine1 = new Text2();
		LineReader in1 = new LineReader(fs.open(currentMasterPath));
		while (in1.readLine(tempLine1) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine1);
			currentPartitions.add(tempPartition);
		}

		int maxCellId = -1;
		for (Partition partition : currentPartitions) {
			if (partition.cellId > maxCellId) {
				maxCellId = partition.cellId;
			}
		}

		long t1 = System.currentTimeMillis();
		Vector<Thread> threads = new Vector<Thread>();
		cells = new ArrayList<Partition>();
		final ArrayList<Partition> cellsFinal = new ArrayList<Partition>();
		for (Partition partition : currentPartitions) {
			if (partition.size >= overflowSize) {
				final Path inPathFinal = inPath;
				final String partitionNameFinal = partition.filename;
				final Path outFinal = out;
				final Configuration confFinal = conf;
				final OperationsParams paramsFinal = params;
				final Partition partitionFinal = partition;
				Thread thread = new Thread() {
					@Override
					public void run() {
						// Get sample to create sub-partitioner
						// int maxCellId;
						try {
							ArrayList<Partition> cellsFromSample = getCellsFromSample(
									new Path(inPathFinal, partitionNameFinal), outFinal, confFinal, paramsFinal,
									partitionFinal, 0);
							// System.out.println("max cell id = " + maxCellId);
//							System.out.println("cells size = " + cellsFromSample.size());
							synchronized (lock) {
								cellsFinal.addAll(cellsFromSample);
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				thread.start();
				threads.add(thread);
			} else {
				this.cells.add(partition);
			}
		}

		while (!threads.isEmpty()) {
			try {
				Thread thread = threads.firstElement();
				thread.join();
				threads.remove(0);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (Partition partition : cellsFinal) {
			Partition p = new Partition(partition);
			maxCellId++;
			p.cellId = maxCellId;
			this.cells.add(p);
		}

		cellsTree = this.buildCellsTree(cells);

		long t2 = System.currentTimeMillis();
		System.out.println("Total time for sampling in millis: " + (t2 - t1));
	}

	private static ArrayList<Partition> getCellsFromSample(Path in, Path out, Configuration job,
			OperationsParams params, Partition partition, int maxCellId) throws IOException {

		Path[] ins = new Path[1];
		ins[0] = in;
		// long t1 = System.currentTimeMillis();
		// final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job,
		// "mbr");
		final Rectangle inMBR = partition.cellMBR;
		// Determine number of partitions
		long inSize = 0;
		inSize += FileUtil.getPathSize(in.getFileSystem(job), in);
		long estimatedOutSize = (long) (inSize * (1.0 + job.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f)));
		FileSystem outFS = out.getFileSystem(job);
		long outBlockSize = outFS.getDefaultBlockSize(out);

		final List<Point> sample = new ArrayList<Point>();
		float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
		long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);

		ResultCollector<Point> resultCollector = new ResultCollector<Point>() {
			@Override
			public void collect(Point p) {
				sample.add(p.clone());
			}
		};

		OperationsParams params2 = new OperationsParams(job);
		params2.setFloat("ratio", sample_ratio);
		params2.setLong("size", sample_size);
		if (job.get("shape") != null)
			params2.set("shape", job.get("shape"));
		if (job.get("local") != null)
			params2.set("local", job.get("local"));
		params2.setBoolean("local", false);
		params2.setBoolean("background", true);
		params2.setClass("outshape", Point.class, Shape.class);
		Sampler.sample(ins, resultCollector, params2);
		// long t2 = System.currentTimeMillis();
		// System.out.println("Total time for sampling in millis: " + (t2 -
		// t1));

		int partitionCapacity = (int) Math.max(1, Math.floor((double) sample.size() * outBlockSize / estimatedOutSize));
		int numPartitions = Math.max(1, (int) Math.ceil((float) estimatedOutSize / outBlockSize));

		ArrayList<Partition> cellsFromPoints = getCellsFromPoints(inMBR, sample.toArray(new Point[sample.size()]),
				partitionCapacity, partition, maxCellId);

		return cellsFromPoints;
	}

	private static ArrayList<Partition> getCellsFromPoints(Rectangle mbr, Point[] points, int capacity,
			Partition partition, int maxCellId) {
		// TODO Auto-generated method stub
		RTree<Integer, Geometry> tree;
		long t1 = System.currentTimeMillis();
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();

//		System.out.println("Creating tree from points. Number of points = " + points.length);

		List<Point> pointList = new ArrayList<Point>();
		if (points.length > MAXIMUM_POINTS) {
			for (int i = 0; i < points.length; i++) {
				Point p = points[i];
				pointList.add(p);
			}
			Collections.shuffle(pointList);
			for (int i = 0; i < MAXIMUM_POINTS; i++) {
				Point p = pointList.get(i);
				entries.add(new EntryDefault<Integer, Geometry>(i, Geometries.point(p.x, p.y)));
			}
		} else {
			for (int i = 0; i < points.length; i++) {
				Point p = points[i];
				entries.add(new EntryDefault<Integer, Geometry>(i, Geometries.point(p.x, p.y)));
			}
		}

		double capacityDouble = (double) MAXIMUM_POINTS / (double) points.length * (double) capacity;
		capacity = points.length > MAXIMUM_POINTS ? (int) Math.ceil(capacityDouble) : capacity;
//		System.out.println("capacity = " + capacity);

		tree = RTree.star().maxChildren(capacity).create();
		// tree = RTree.maxChildren(capacity).create();
		tree = tree.add(entries);

		long t2 = System.currentTimeMillis();
//		System.out.println("Total adding entries time in millis " + (t2 - t1));

		// Get list of all leaf nodes
		ArrayList<Partition> cells = new ArrayList<Partition>();
		Node<Integer, Geometry> node = tree.root().get();
		List<Rectangle> rects = getAllLeafs(node);
		// int cellId = maxCellId + 1;
		for (Rectangle r : rects) {
			CellInfo cell = new CellInfo(1, r);
			cells.add(new Partition(partition.filename, cell, r));
			// cellId++;
		}
		// maxCellId = maxCellId + rects.size();

		long t3 = System.currentTimeMillis();
//		System.out.println("Total making cell time in millis " + (t3 - t1));

		return cells;
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub

	}

	public void overlapPartitions(String filename, Shape shape, ResultCollector<Integer> matcher) {
		List<Partition> nearestCells = this.getNearestCells(shape, 20);
//		System.out.println("number of nearest cells = " + nearestCells.size());

		// TODO Auto-generated method stub
		ArrayList<Partition> partitionCells = new ArrayList<Partition>();
		for (Partition p : nearestCells) {
			if (p.filename.equals(filename)) {
				partitionCells.add(p);
			}
		}

		boolean found = false;
		for (CellInfo cell : partitionCells) {
			if (cell.isIntersected(shape)) {
				matcher.collect(cell.cellId);
				found = true;
			}
		}
		if (!found) {
			double minimumExpansion = MINIMUM_EXPANSION;
			CellInfo minimumCell = (CellInfo) partitionCells.get(0);
			for (CellInfo cell : partitionCells) {
				CellInfo tempCell = new CellInfo(cell);
				tempCell.expand(shape);
				double expansionArea = tempCell.getSize() - cell.getSize();
				if (expansionArea < minimumExpansion) {
					minimumExpansion = expansionArea;
					minimumCell = cell;
				}
			}
			matcher.collect(minimumCell.cellId);
		}
	}

	@Override
	public int overlapPartition(Shape shape) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int overlapPartition(String filename, Shape shape) {
		List<Partition> overlappingCells = this.getOverlappingCells(shape);
//		System.out.println("Number of overlapping cell = " + overlappingCells.size());
		if(overlappingCells.size() > 0) {
			ArrayList<Partition> partitionCells = new ArrayList<Partition>();
			for (Partition p : overlappingCells) {
				if (p.filename.equals(filename)) {
					partitionCells.add(p);
				}
			}
			for (CellInfo cell : partitionCells) {
				if (cell.isIntersected(shape)) {
//					System.out.println("return intersected cell = " + cell.cellId);
					return cell.cellId;
				}
			}
		} else {
			List<Partition> nearestCells = this.getNearestCells(shape, 10);
			ArrayList<Partition> partitionCells = new ArrayList<Partition>();
			for (Partition p : nearestCells) {
				if (p.filename.equals(filename)) {
					partitionCells.add(p);
				}
			}
//			System.out.println("number of nearest cells = " + nearestCells.size());

			if(partitionCells.size() > 0) {
//				System.out.println("number of partitionCells = " + partitionCells.size());
				double minimumExpansion = MINIMUM_EXPANSION;
				CellInfo minimumCell = partitionCells.get(0);
				for (CellInfo cell : partitionCells) {
					CellInfo tempCell = new CellInfo(cell);
					tempCell.expand(shape);
					double expansionArea = tempCell.getSize() - cell.getSize();
					if (expansionArea < minimumExpansion) {
						minimumExpansion = expansionArea;
						minimumCell = cell;
					}
				}
//				System.out.println("return minimum expand cell = " + minimumCell.cellId);
				return minimumCell.cellId;
			}
		}
//		System.out.println("Return first cell. Should not run to here ");
		return this.cells.get(0).cellId;
//		List<Partition> nearestCells = this.getNearestCells(shape, 20);
//		System.out.println("number of nearest cells = " + nearestCells.size());
//
//		// TODO Auto-generated method stub
//		ArrayList<Partition> partitionCells = new ArrayList<Partition>();
//		for (Partition p : nearestCells) {
//			if (p.filename.equals(filename)) {
//				partitionCells.add(p);
//			}
//		}
//
//		for (CellInfo cell : partitionCells) {
//			if (cell.isIntersected(shape)) {
//				System.out.println("Matched cell ID = " + cell.cellId);
//				return cell.cellId;
//			}
//		}
//
//		if (partitionCells.size() > 0)
//			return partitionCells.get(0).cellId;
//		else if (nearestCells.size() > 0)
//			return nearestCells.get(0).cellId;
//		else
//			return this.cells.get(0).cellId;
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		CellInfo result = new CellInfo(partitionID, 0, 0, 0, 0);
		List<Partition> matchingCells = this.cells.stream().filter(c -> c.cellId == partitionID)
				.collect(Collectors.toList());
		for (CellInfo cell : matchingCells) {
			if (cell.cellId == partitionID) {
				result = cell;
				break;
			}
		}
		return result;
	}

	@Override
	public CellInfo getPartitionAt(int index) {
		// TODO Auto-generated method stub
		return this.cells.get(index);
	}

	@Override
	public int getPartitionCount() {
		// TODO Auto-generated method stub
		return this.cells.size();
	}

	private static <T, S extends Geometry> List<Rectangle> getAllLeafs(Node<T, S> node) {
		List<Rectangle> leafRects = new ArrayList<Rectangle>();
		if (node instanceof Leaf) {
			final Leaf<T, S> leaf = (Leaf<T, S>) node;
			Rectangle rect = new Rectangle(leaf.geometry().mbr().x1(), leaf.geometry().mbr().y1(),
					leaf.geometry().mbr().x2(), leaf.geometry().mbr().y2());
			leafRects.add(rect);
		} else {
			final NonLeaf<T, S> n = (NonLeaf<T, S>) node;
			for (int i = 0; i < n.count(); i++) {
				leafRects.addAll(getAllLeafs(n.child(i)));
			}
		}

		return leafRects;
	}

}
