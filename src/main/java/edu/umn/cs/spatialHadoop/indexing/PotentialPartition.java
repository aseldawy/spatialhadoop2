package edu.umn.cs.spatialHadoop.indexing;

import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;

public class PotentialPartition extends Partition {

	public ArrayList<IntersectionInfo> intersections;
	public double overlappingArea = 0;

	public PotentialPartition(Partition other) {
		this.filename = other.filename;
		this.recordCount = other.recordCount;
		this.size = other.size;
		this.cellMBR.set(other.cellMBR);
		super.set((CellInfo) other);
		intersections = new ArrayList<IntersectionInfo>();
	}
	
	public double getRepartitionBenefit(long blockSize) {
		int currentBlocks = (int)(this.size / blockSize);
		double currentValue = this.getSize() * currentBlocks;
		
		double overlappingValue = 0;
		for(IntersectionInfo inter: this.intersections) {
			double sizeRatio = inter.getCell().getSize() / this.getSize();
			double interBlocks = (double)(sizeRatio * currentBlocks);
			if(interBlocks < 1) {
				interBlocks = 1;
			} else {
				interBlocks = Math.ceil(interBlocks);
			}
			double interValue = inter.getCell().getSize() * interBlocks;
			overlappingValue += interValue;
		}
		
		return (currentValue - overlappingValue) / currentBlocks;
	}
}
