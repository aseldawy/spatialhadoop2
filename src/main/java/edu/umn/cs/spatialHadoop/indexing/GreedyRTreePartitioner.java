package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

public class GreedyRTreePartitioner extends Partitioner {

	private static final double MINIMUM_EXPANSION = Double.MAX_VALUE;
	ArrayList<RTreePartitioner> partitioners = new ArrayList<RTreePartitioner>();
	public ArrayList<CellInfo> cells = new ArrayList<CellInfo>();
	
	public GreedyRTreePartitioner(){}

	public GreedyRTreePartitioner(ArrayList<RTreePartitioner> partitioners, int maxCellId) {
		// TODO Auto-generated constructor stub
		this.partitioners = partitioners;
		for (RTreePartitioner partitioner : partitioners) {
			for (CellInfo cell : partitioner.cells) {
				maxCellId++;
				cell.cellId = maxCellId;
				CellInfo cellInfo = new CellInfo(cell);
				this.cells.add(cellInfo);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cells.size());
		for (CellInfo cell : this.cells) {
			cell.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		int cellsSize = in.readInt();
		cells = new ArrayList<CellInfo>(cellsSize);
		for (int i = 0; i < cellsSize; i++) {
			CellInfo cellInfo = new CellInfo();
			cellInfo.readFields(in);
			cells.add(cellInfo);
		}
	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int capacity) throws IllegalArgumentException {
		// TODO Auto-generated method stub

	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub

	}

	public void overlapPartitions(String filename, Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub

		boolean found = false;
		for(CellInfo cell: this.cells) {
			if(cell.isIntersected(shape)) {
				matcher.collect(cell.cellId);
				found = true;
			}
		}
		if(!found) {
			double minimumExpansion = MINIMUM_EXPANSION;
			CellInfo minimumCell = this.cells.get(0);
			for(CellInfo cell: this.cells) {
				CellInfo tempCell = new CellInfo(cell);
				tempCell.expand(shape);
				double expansionArea = tempCell.getSize() - cell.getSize();
				if(expansionArea < minimumExpansion) {
					minimumExpansion = expansionArea;
					minimumCell = cell;
				}
			}
			matcher.collect(minimumCell.cellId);
		}
	}

	public int overlapPartition(String filename, Shape shape) {
		// TODO Auto-generated method stub
		for(CellInfo cell: this.cells) {
			if(cell.isIntersected(shape)) {
				return cell.cellId;
			}
		}
		
		double minimumExpansion = MINIMUM_EXPANSION;
		CellInfo minimumCell = this.cells.get(0);
		for(CellInfo cell: this.cells) {
			CellInfo tempCell = new CellInfo(cell);
			tempCell.expand(shape);
			double expansionArea = tempCell.getSize() - cell.getSize();
			if(expansionArea < minimumExpansion) {
				minimumExpansion = expansionArea;
				minimumCell = cell;
			}
		}
		
		return minimumCell.cellId;
	}

	@Override
	public int overlapPartition(Shape shape) {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		CellInfo result = new CellInfo(partitionID, 0, 0, 0, 0);
		for (CellInfo cell : this.cells) {
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
