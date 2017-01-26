package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.CellInfo;

public class IntersectionInfo {
	private CellInfo cell;
	private double jsValue;

	public IntersectionInfo(CellInfo cell, double jsValue) {
		this.setCell(cell);
		this.setJsValue(jsValue);
	}

	public double getJsValue() {
		return jsValue;
	}

	public void setJsValue(double jsValue) {
		this.jsValue = jsValue;
	}

	public CellInfo getCell() {
		return cell;
	}

	public void setCell(CellInfo cell) {
		this.cell = cell;
	}
}
