package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;


import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.EntryDefault;
import com.github.davidmoten.rtree.RTree;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

public class RTreePartitioner extends Partitioner {

	private static final double MINIMUM_EXPANSION = Double.MAX_VALUE;
	private static final int MAXIMUM_POINTS = 50000;
	private RTree<Integer, Geometry> tree;
//	private RTree<Integer, Geometry> treeOfLeafs;
	public ArrayList<CellInfo> cells = new ArrayList<CellInfo>();
	private RTree<Integer, Geometry> cellsTree;
	
	private RTree<Integer, Geometry> buildCellsTree(ArrayList<CellInfo> cellList) {
		System.out.println("Building tree of cells.");
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();
		for (CellInfo cell : cellList) {
			Rectangle r = cell.getMBR();
			entries.add(new EntryDefault<Integer, Geometry>(cell.cellId, Geometries.rectangle(r.x1, r.y1, r.x2, r.y2)));
		}

		RTree<Integer, Geometry> tree = RTree.star().maxChildren(10).create();
		tree = tree.add(entries);
		System.out.println("Tree depth = " + tree.calculateDepth());

		return tree;
	}
	
	private List<CellInfo> getNearestCells(Shape shape, int maxCount) {
//		System.out.println("Getting nearest cells");
		ArrayList<Integer> nearestCellIds = new ArrayList<Integer>();

		Rectangle r = shape.getMBR();
		List<Entry<Integer, Geometry>> entries = this.cellsTree
				.nearest(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2), 50, maxCount).toList().toBlocking().single();
//		List<Entry<Integer, Geometry>> entries = this.cellsTree.search(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2))
//				.toList().toBlocking().single();
		for (Entry<Integer, Geometry> entry : entries) {
			Integer cellId = entry.value();
			nearestCellIds.add(cellId);
		}
		List<CellInfo> nearestCells = this.cells.stream().filter(c -> nearestCellIds.contains(new Integer(c.cellId)))
				.collect(Collectors.toList());

		return nearestCells;
		// return null;
	}
	
	private List<CellInfo> getOverlappingCells(Shape shape) {
		ArrayList<Integer> overlappingCellIds = new ArrayList<Integer>();
		Rectangle r = shape.getMBR();
		
		List<Entry<Integer, Geometry>> entries = this.cellsTree.search(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2))
				.toList().toBlocking().single();
		for (Entry<Integer, Geometry> entry : entries) {
			Integer cellId = entry.value();
			overlappingCellIds.add(cellId);
		}
		List<CellInfo> overlappingCells = this.cells.stream().filter(c -> overlappingCellIds.contains(new Integer(c.cellId)))
				.collect(Collectors.toList());

		return overlappingCells;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cells.size());
		for(CellInfo cell: this.cells) {
			cell.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int cellsSize = in.readInt();
		cells = new ArrayList<CellInfo>(cellsSize);
		for (int i = 0; i < cellsSize; i++) {
			CellInfo cellInfo = new CellInfo();
			cellInfo.readFields(in);
			cells.add(cellInfo);
		}
		cellsTree = this.buildCellsTree(cells);
	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int capacity) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		long t1 = System.currentTimeMillis();
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();
		
		System.out.println("Creating tree from points. Number of points = " + points.length);
		
		List<Point> pointList = new ArrayList<Point>();
		if(points.length > MAXIMUM_POINTS) {
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
		
		double capacityDouble = (double)MAXIMUM_POINTS / (double)points.length * (double)capacity;
		capacity = points.length > MAXIMUM_POINTS ? (int) Math.ceil(capacityDouble) : capacity;
		System.out.println("capacity = " + capacity);

		tree = RTree.star().maxChildren(capacity).create();
//		tree = RTree.maxChildren(capacity).create();
		tree = tree.add(entries);
		
		long t2 = System.currentTimeMillis();
		System.out.println("Total adding entries time in millis "+(t2-t1));
		
		// Get list of all leaf nodes
		Node<Integer, Geometry> node = tree.root().get();
		List<Rectangle> rects = getAllLeafs(node);
		cells = new ArrayList<CellInfo>();
		int cellId = 1;
		for(Rectangle r: rects) {
			cells.add(new CellInfo(cellId, r));
			cellId++;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println("Total making cell time in millis "+(t3-t1));
		cellsTree = this.buildCellsTree(cells);
		
//		cells.add(new CellInfo(cellId, mbr));
		
		// Build the tree of leafs
//		List<Entry<Integer, Geometry>> leafEntries = new ArrayList<Entry<Integer, Geometry>>();
//		for(int i = 0; i < rects.size(); i++) {
//			Rectangle rect = rects.get(i);
//			leafEntries.add(new EntryDefault<Integer, Geometry>(i, Geometries.rectangle(rect.x1, rect.y1, rect.x2, rect.y2)));
//		}
//		treeOfLeafs = RTree.star().maxChildren(capacity).create();
//		treeOfLeafs = treeOfLeafs.add(leafEntries);
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub
		boolean found = false;
		for(CellInfo cell: this.cells) {
			if(cell.isIntersected(shape)) {
				matcher.collect(cell.cellId);
				found = true;
			}
		}
		if(!found) {
//			double minimumExpansion = MINIMUM_EXPANSION;
//			CellInfo minimumCell = this.cells.get(0);
//			for(CellInfo cell: this.cells) {
//				CellInfo tempCell = new CellInfo(cell);
//				tempCell.expand(shape);
//				double expansionArea = tempCell.getSize() - cell.getSize();
//				if(expansionArea < minimumExpansion) {
//					minimumExpansion = expansionArea;
//					minimumCell = cell;
//				}
//			}
//			matcher.collect(minimumCell.cellId);
			matcher.collect(this.cells.get(0).cellId);
		}
	}

	@Override
	public int overlapPartition(Shape shape) {
		List<CellInfo> overlappingCells = this.getOverlappingCells(shape);
//		System.out.println("Number of overlapping cell = " + overlappingCells.size());
		if(overlappingCells.size() > 0) {
			Random random  = new Random();
			int randomCellIndex = random.nextInt(overlappingCells.size());
			return overlappingCells.get(randomCellIndex).cellId;
//			for (CellInfo cell : overlappingCells) {
//				if (cell.isIntersected(shape)) {
////					System.out.println("return intersected cell = " + cell.cellId);
//					return cell.cellId;
//				}
//			}
		} else {
			List<CellInfo> nearestCells = this.getNearestCells(shape, 10);
//			System.out.println("number of nearest cells = " + nearestCells.size());

			if(nearestCells.size() > 0) {
				double minimumExpansion = MINIMUM_EXPANSION;
				CellInfo minimumCell = nearestCells.get(0);
				for (CellInfo cell : nearestCells) {
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
//		for(CellInfo cell: this.cells) {
//			if(cell.isIntersected(shape)) {
//				return cell.cellId;
//			}
//		}
		
		// If there is no overlapping partitions
//		double minimumExpansion = MINIMUM_EXPANSION;
//		CellInfo minimumCell = this.cells.get(0);
//		for(CellInfo cell: this.cells) {
//			CellInfo tempCell = new CellInfo(cell);
//			tempCell.expand(shape);
//			double expansionArea = tempCell.getSize() - cell.getSize();
//			if(expansionArea < minimumExpansion) {
//				minimumExpansion = expansionArea;
//				minimumCell = cell;
//			}
//		}
//		
//		return minimumCell.cellId;
//		return this.cells.get(0).cellId;
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		CellInfo result = new CellInfo(partitionID, 0, 0, 0, 0);
		for (CellInfo cell: this.cells) {
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

	private <T, S extends Geometry> List<Rectangle> getAllLeafs(Node<T, S> node) {
		List<Rectangle> leafRects = new ArrayList<Rectangle>();
		if(node instanceof Leaf) {
			final Leaf<T, S> leaf = (Leaf<T, S>) node;
			Rectangle rect = new Rectangle(leaf.geometry().mbr().x1(), leaf.geometry().mbr().y1(), leaf.geometry().mbr().x2(), leaf.geometry().mbr().y2());
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
