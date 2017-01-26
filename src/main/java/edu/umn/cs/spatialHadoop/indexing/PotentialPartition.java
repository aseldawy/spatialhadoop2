package edu.umn.cs.spatialHadoop.indexing;

import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;

public class PotentialPartition extends Partition {

	public ArrayList<IntersectionInfo> intersections;

	public PotentialPartition(Partition other) {
		this.filename = other.filename;
		this.recordCount = other.recordCount;
		this.size = other.size;
		this.cellMBR.set(other.cellMBR);
		super.set((CellInfo) other);
		intersections = new ArrayList<IntersectionInfo>();
	}
}
