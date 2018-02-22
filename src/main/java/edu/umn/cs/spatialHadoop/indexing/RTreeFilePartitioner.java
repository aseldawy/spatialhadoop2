package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.LineReader;

import com.github.davidmoten.rtree.Entry;
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
import edu.umn.cs.spatialHadoop.io.Text2;

public class RTreeFilePartitioner extends Partitioner {

	private static final double MINIMUM_EXPANSION = Double.MAX_VALUE;
	private static final int MAXIMUM_NEAREST_CELLS = 3;
	protected ArrayList<CellInfo> cells;
	private RTree<Integer, Geometry> cellsTree;

	public RTreeFilePartitioner() {
		// TODO Auto-generated constructor stub
		cells = new ArrayList<CellInfo>();
		cellsTree = this.buildCellsTree(cells);
	}

	private RTree<Integer, Geometry> buildCellsTree(ArrayList<CellInfo> cellList) {
		System.out.println("Building tree of " + cellList.size() + " cells.");
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
		ArrayList<Integer> nearestCellIds = new ArrayList<Integer>();

		Rectangle r = shape.getMBR();
		List<Entry<Integer, Geometry>> entries = this.cellsTree
				.nearest(Geometries.rectangle(r.x1, r.y1, r.x2, r.y2), 50, maxCount).toList().toBlocking().single();
		for (Entry<Integer, Geometry> entry : entries) {
			Integer cellId = entry.value();
			nearestCellIds.add(cellId);
		}
		List<CellInfo> nearestCells = this.cells.stream().filter(c -> nearestCellIds.contains(new Integer(c.cellId)))
				.collect(Collectors.toList());

		return nearestCells;
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
		out.writeInt(cells.size());
		for (CellInfo cell : this.cells) {
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

	}

	/**
	 * Create this partitioner based on information from master file
	 * 
	 * @param inPath
	 * @param params
	 * @throws IOException
	 */
	public void createFromMasterFile(Path inPath, OperationsParams params) throws IOException {
		this.cells = new ArrayList<CellInfo>();
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(inPath, params);
		for(Partition p: partitions) {
			CellInfo tempCellInfo = new CellInfo();
			tempCellInfo.set(p.cellMBR);
			tempCellInfo.cellId = p.cellId;
			this.cells.add(tempCellInfo);
		}
		cellsTree = this.buildCellsTree(cells);
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		System.out.println("overlapPartitions method");
		List<CellInfo> nearestCells = this.getNearestCells(shape, MAXIMUM_NEAREST_CELLS);
		System.out.println("number of nearest cells = " + nearestCells.size());

		// TODO Auto-generated method stub
		boolean found = false;
		for (CellInfo cell : nearestCells) {
			if (cell.isIntersected(shape)) {
				matcher.collect(cell.cellId);
				found = true;
			}
		}
		if (!found) {
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
			matcher.collect(minimumCell.cellId);
		}
	}

	@Override
	public int overlapPartition(Shape shape) {
		List<CellInfo> overlappingCells = this.getOverlappingCells(shape);
		int numberOfOverlappingCells = overlappingCells.size();
//		System.out.println("Number of overlapping cell = " + numberOfOverlappingCells);
		if(numberOfOverlappingCells > 0) {
//			Random random = new Random();
//			int index = random.nextInt(numberOfOverlappingCells - 1);
//			return overlappingCells.get(index).cellId;
			for (CellInfo cell : overlappingCells) {
				if (cell.isIntersected(shape)) {
//					System.out.println("return intersected cell = " + cell.cellId);
					return cell.cellId;
				}
			}
		} else {
			List<CellInfo> nearestCells = this.getNearestCells(shape, MAXIMUM_NEAREST_CELLS);
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
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		CellInfo result = new CellInfo(partitionID, 0, 0, 0, 0);
		List<CellInfo> matchingCells = this.cells.stream().filter(c -> c.cellId == partitionID)
				.collect(Collectors.toList());
//		System.out.println("matching cell size = " + matchingCells.size());
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

}
