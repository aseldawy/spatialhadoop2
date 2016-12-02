package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

public class FilePartitioner extends Partitioner {

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int capacity) throws IllegalArgumentException {
		// TODO Auto-generated method stub

	}
	
	public void createFromFile(Path wktFile) {
		
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub

	}

	@Override
	public int overlapPartition(Shape shape) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CellInfo getPartitionAt(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPartitionCount() {
		// TODO Auto-generated method stub
		return 0;
	}

}
