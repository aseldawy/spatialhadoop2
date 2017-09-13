package edu.umn.cs.spatialHadoop.util;

public class Node {
	private String label;
	private double weight;
	private double totalEdgeWeight;
	private double weightRatio;
	private double density;
	
	public Node(String label, double weight) {
		this.setLabel(label);
		this.setWeight(weight);
		this.setTotalEdgeWeight(0);
		this.setWeightRatio(0);
	}
	
	public Node(String label, double weight, double density) {
		this.setLabel(label);
		this.setWeight(weight);
		this.density = density;
		this.setTotalEdgeWeight(0);
		this.setWeightRatio(0);
	}

	public double getDensity() {
		return density;
	}

	public void setDensity(double density) {
		this.density = density;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getTotalEdgeWeight() {
		return totalEdgeWeight;
	}

	public void setTotalEdgeWeight(double totalEdgeWeight) {
		this.totalEdgeWeight = totalEdgeWeight;
	}

	public double getWeightRatio() {
		return weightRatio;
	}

	public void setWeightRatio(double weightRatio) {
		this.weightRatio = weightRatio;
	}
}