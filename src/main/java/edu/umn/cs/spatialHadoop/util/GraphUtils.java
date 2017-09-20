package edu.umn.cs.spatialHadoop.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

public class GraphUtils {
	public static Set<Node> findMaximalWeightedSubgraph(SimpleWeightedGraph<Node, DefaultWeightedEdge> graph,
			double budget) {
		Set<Node> solution = new HashSet<Node>();
		int numberOfNodes = graph.vertexSet().size();
		List<List<Node>> plots = new ArrayList<List<Node>>(numberOfNodes);

		for (Node v : graph.vertexSet()) {
			Set<Node> F = new HashSet<Node>();
			F.add(v);
			List<Node> plot = new ArrayList<Node>();
			v.setWeightRatio(0);
			plot.add(v);

			while (getTotalNodeWeight(F) < budget) {
				// Find all neighbors of F
				Set<Node> neighborsF = new HashSet<Node>();
				for (Node u : F) {
					List<Node> neighborsU = Graphs.neighborListOf(graph, u);
					for (Node w : neighborsU) {
						if (!F.contains(w)) {
							neighborsF.add(w);
						}
					}
				}

				if(neighborsF.size() > 0) {
					// Compute total edge's weights of all neighbor of F
					for (Node w : neighborsF) {
						double totalEdgeWeight = 0;
						for (Node u : F) {
							if (graph.containsEdge(u, w)) {
								totalEdgeWeight += graph.getEdgeWeight(graph.getEdge(u, w));
							}
						}
						w.setTotalEdgeWeight(totalEdgeWeight);
					}

					// Find the node with highest ratio of total edge's weights over
					// node's weight
					Node selectedNode = new Node("", 1);
					selectedNode.setTotalEdgeWeight(0);
					for (Node w : neighborsF) {
						if (selectedNode.getTotalEdgeWeight() / selectedNode.getWeight() < w.getTotalEdgeWeight()
								/ w.getWeight()) {
							selectedNode = w;
						}
					}
					if(getTotalNodeWeight(F) + selectedNode.getWeight() <= budget) {
						F.add(selectedNode);
						Node cloneNode = new Node(selectedNode.getLabel(), selectedNode.getWeight());
						cloneNode.setWeightRatio(getTotalEdgeWeight(graph, F) / getTotalNodeWeight(F));
						plot.add(cloneNode);
					} else {
						break;
					}
				} else {
					break;
				}
			}
			plots.add(plot);
		}

		// Find the plot with maximal value of weight ratio
		List<Node> maximalPlot = plots.get(0);
		for (List<Node> p : plots) {
			if (maximalPlot.get(maximalPlot.size() - 1).getWeightRatio() < p.get(p.size() - 1).getWeightRatio()) {
				maximalPlot = p;
			}
		}
		
		for(Node node: maximalPlot) {
			solution.add(node);
		}

		return solution;
	}
	
	public static Set<Node> findMaximalMultipleWeightedSubgraphs(SimpleWeightedGraph<Node, DefaultWeightedEdge> graph,
			double budget) {
		Set<Node> solution = new HashSet<Node>();
		while (getTotalNodeWeight(solution) < budget) {
			int numberOfNodes = graph.vertexSet().size();
			List<List<Node>> plots = new ArrayList<List<Node>>(numberOfNodes);

			for (Node v : graph.vertexSet()) {
				Set<Node> F = new HashSet<Node>();
				F.add(v);
				List<Node> plot = new ArrayList<Node>();
				v.setWeightRatio(0);
				plot.add(v);

				while (getTotalNodeWeight(F) < budget) {
					// Find all neighbors of F
					Set<Node> neighborsF = new HashSet<Node>();
					for (Node u : F) {
						List<Node> neighborsU = Graphs.neighborListOf(graph, u);
						for (Node w : neighborsU) {
							if (!F.contains(w)) {
								neighborsF.add(w);
							}
						}
					}

					if(neighborsF.size() > 0) {
						// Compute total edge's weights of all neighbor of F
						for (Node w : neighborsF) {
							double totalEdgeWeight = 0;
							for (Node u : F) {
								if (graph.containsEdge(u, w)) {
									totalEdgeWeight += graph.getEdgeWeight(graph.getEdge(u, w));
								}
							}
							w.setTotalEdgeWeight(totalEdgeWeight);
						}

						// Find the node with highest ratio of total edge's weights over
						// node's weight
						Node selectedNode = new Node("", 1);
						selectedNode.setTotalEdgeWeight(0);
						for (Node w : neighborsF) {
							if (selectedNode.getTotalEdgeWeight() / selectedNode.getWeight() < w.getTotalEdgeWeight()
									/ w.getWeight()) {
								selectedNode = w;
							}
						}
						if(getTotalNodeWeight(F) + selectedNode.getWeight() <= budget) {
							F.add(selectedNode);
							Node cloneNode = new Node(selectedNode.getLabel(), selectedNode.getWeight());
							cloneNode.setWeightRatio(getTotalEdgeWeight(graph, F) / getTotalNodeWeight(F));
							plot.add(cloneNode);
						} else {
							break;
						}
					} else {
						break;
					}
				}
				plots.add(plot);
			}

			// Find the position in a plot with maximal value of weight ratio
			List<Node> maximalPlot = plots.get(0);
			int maximalIndex = 0;
			Node maximalNode = maximalPlot.get(maximalIndex);
			for (List<Node> p : plots) {
				for(int i = 0; i < p.size(); i++) {
					Node currentNode = p.get(i);
					if(maximalNode.getWeightRatio() < currentNode.getWeightRatio()) {
						maximalNode = currentNode;
						maximalPlot = p; 
						maximalIndex = i;
					}
				}
			}
			
			Set<Node> subComponent = new HashSet<Node>();
			for(int i = 0; i <= maximalIndex; i++) {
				subComponent.add(maximalPlot.get(i));
			}
			
			Set<Node> verticesToRemove = new HashSet<Node>();
			for(Node n1: graph.vertexSet()) {
				for(Node n2: subComponent) {
					if(n1.getLabel().equals(n2.getLabel())) {
						verticesToRemove.add(n1);
					}
				}
			}
			
			if(getTotalNodeWeight(solution) + getTotalNodeWeight(verticesToRemove) <= budget) {
				// Add new sub-component to the solution then remove it from current graph
				solution.addAll(verticesToRemove);
				graph.removeAllVertices(verticesToRemove);
			} else {
				break;
			}
		}

		return solution;
	}
	
	public static Set<Node> findMaximalMultipleWeightedSubgraphs2(SimpleWeightedGraph<Node, DefaultWeightedEdge> graph,
			double budget) {
		Set<Node> solution = new HashSet<Node>();
		while (getTotalNodeWeight(solution) < budget) {
			int numberOfNodes = graph.vertexSet().size();
			List<List<Node>> plots = new ArrayList<List<Node>>(numberOfNodes);

			for (Node v : graph.vertexSet()) {
				Set<Node> F = new HashSet<Node>();
				F.add(v);
				List<Node> plot = new ArrayList<Node>();
				v.setWeightRatio(0);
				plot.add(v);

				while (getTotalNodeWeight(F) < budget) {
					// Find all neighbors of F
					Set<Node> neighborsF = new HashSet<Node>();
					for (Node u : F) {
//						System.out.println("u = " + u.getLabel());
						List<Node> neighborsU = Graphs.neighborListOf(graph, u);
						for (Node w : neighborsU) {
							if (!F.contains(w)) {
								neighborsF.add(w);
							}
						}
					}

					if(neighborsF.size() > 0) {
						// Compute total edge's weights of all neighbor of F
						for (Node w : neighborsF) {
							double totalEdgeWeight = 0;
							for (Node u : F) {
								if (graph.containsEdge(u, w)) {
									totalEdgeWeight += graph.getEdgeWeight(graph.getEdge(u, w));
								}
							}
							w.setTotalEdgeWeight(totalEdgeWeight);
						}

						// Find the node with highest ratio of total edge's weights over
						// node's density
						Node selectedNode = new Node("", 1, 1);
						selectedNode.setTotalEdgeWeight(0);
						for (Node w : neighborsF) {
//							if (selectedNode.getTotalEdgeWeight() / selectedNode.getWeight() < w.getTotalEdgeWeight()
//									/ w.getWeight()) {
//								selectedNode = w;
//							}
							if (selectedNode.getTotalEdgeWeight() / selectedNode.getDensity() < w.getTotalEdgeWeight()
									/ w.getDensity()) {
								selectedNode = w;
							}
						}
						if(getTotalNodeWeight(F) + selectedNode.getWeight() <= budget) {
							F.add(selectedNode);
							Node cloneNode = new Node(selectedNode.getLabel(), selectedNode.getWeight(), selectedNode.getDensity());
							cloneNode.setWeightRatio(getTotalEdgeWeight(graph, F) / getTotalNodeDensity(F));
							plot.add(cloneNode);
						} else {
							break;
						}
					} else {
						break;
					}
				}
				plots.add(plot);
			}

			// Find the position in a plot with maximal value of weight ratio
			List<Node> maximalPlot = plots.get(0);
			int maximalIndex = 0;
			Node maximalNode = maximalPlot.get(maximalIndex);
			for (List<Node> p : plots) {
				for(int i = 0; i < p.size(); i++) {
					Node currentNode = p.get(i);
					if(maximalNode.getWeightRatio() < currentNode.getWeightRatio()) {
						maximalNode = currentNode;
						maximalPlot = p; 
						maximalIndex = i;
					}
				}
			}
			
			Set<Node> subComponent = new HashSet<Node>();
			for(int i = 0; i <= maximalIndex; i++) {
				subComponent.add(maximalPlot.get(i));
			}
			
			Set<Node> verticesToRemove = new HashSet<Node>();
			for(Node n1: graph.vertexSet()) {
				for(Node n2: subComponent) {
					if(n1.getLabel().equals(n2.getLabel())) {
						verticesToRemove.add(n1);
					}
				}
			}
			
			if(getTotalNodeWeight(solution) + getTotalNodeWeight(verticesToRemove) <= budget) {
				// Add new sub-component to the solution then remove it from current graph
				solution.addAll(verticesToRemove);
				graph.removeAllVertices(verticesToRemove);
			} else {
				break;
			}
		}

		return solution;
	}
	
	public static ArrayList<Set<Node>> findMaximalMultipleWeightedSubgraphs3(SimpleWeightedGraph<Node, DefaultWeightedEdge> graph,
			double budget) {
		Set<Node> solution = new HashSet<Node>();
		ArrayList<Set<Node>> setSolution = new ArrayList<Set<Node>>();
		
		while (getTotalNodeWeight(solution) < budget) {
			int numberOfNodes = graph.vertexSet().size();
			List<List<Node>> plots = new ArrayList<List<Node>>(numberOfNodes);

			for (Node v : graph.vertexSet()) {
				Set<Node> F = new HashSet<Node>();
				F.add(v);
				List<Node> plot = new ArrayList<Node>();
				v.setWeightRatio(0);
				plot.add(v);

				while (getTotalNodeWeight(F) < budget) {
					// Find all neighbors of F
					Set<Node> neighborsF = new HashSet<Node>();
					for (Node u : F) {
//						System.out.println("u = " + u.getLabel());
						List<Node> neighborsU = Graphs.neighborListOf(graph, u);
						for (Node w : neighborsU) {
							if (!F.contains(w)) {
								neighborsF.add(w);
							}
						}
					}

					if(neighborsF.size() > 0) {
						// Compute total edge's weights of all neighbor of F
						for (Node w : neighborsF) {
							double totalEdgeWeight = 0;
							for (Node u : F) {
								if (graph.containsEdge(u, w)) {
									totalEdgeWeight += graph.getEdgeWeight(graph.getEdge(u, w));
								}
							}
							w.setTotalEdgeWeight(totalEdgeWeight);
						}

						// Find the node with highest ratio of total edge's weights over
						// node's density
						Node selectedNode = new Node("", 1, 1);
						selectedNode.setTotalEdgeWeight(0);
						for (Node w : neighborsF) {
//							if (selectedNode.getTotalEdgeWeight() / selectedNode.getWeight() < w.getTotalEdgeWeight()
//									/ w.getWeight()) {
//								selectedNode = w;
//							}
							if (selectedNode.getTotalEdgeWeight() / selectedNode.getDensity() < w.getTotalEdgeWeight()
									/ w.getDensity()) {
								selectedNode = w;
							}
						}
						if(getTotalNodeWeight(F) + selectedNode.getWeight() <= budget) {
							F.add(selectedNode);
							Node cloneNode = new Node(selectedNode.getLabel(), selectedNode.getWeight(), selectedNode.getDensity());
							cloneNode.setWeightRatio(getTotalEdgeWeight(graph, F) / getTotalNodeDensity(F));
							plot.add(cloneNode);
						} else {
							break;
						}
					} else {
						break;
					}
				}
				plots.add(plot);
			}

			// Find the position in a plot with maximal value of weight ratio
			List<Node> maximalPlot = plots.get(0);
			int maximalIndex = 0;
			Node maximalNode = maximalPlot.get(maximalIndex);
			for (List<Node> p : plots) {
				for(int i = 0; i < p.size(); i++) {
					Node currentNode = p.get(i);
					if(maximalNode.getWeightRatio() < currentNode.getWeightRatio()) {
						maximalNode = currentNode;
						maximalPlot = p; 
						maximalIndex = i;
					}
				}
			}
			
			Set<Node> subComponent = new HashSet<Node>();
			for(int i = 0; i <= maximalIndex; i++) {
				subComponent.add(maximalPlot.get(i));
			}
			
			Set<Node> verticesToRemove = new HashSet<Node>();
			for(Node n1: graph.vertexSet()) {
				for(Node n2: subComponent) {
					if(n1.getLabel().equals(n2.getLabel())) {
						verticesToRemove.add(n1);
					}
				}
			}
			
			if(getTotalNodeWeight(solution) + getTotalNodeWeight(verticesToRemove) <= budget) {
				// Add new sub-component to the solution then remove it from current graph
				solution.addAll(verticesToRemove);
				setSolution.add(verticesToRemove);
				graph.removeAllVertices(verticesToRemove);
			} else {
				break;
			}
		}

		return setSolution;
	}

	public static double getTotalNodeWeight(Collection<Node> nodes) {
		double totalNodeWeight = 0;
		for (Node node : nodes) {
			totalNodeWeight += node.getWeight();
		}
		return totalNodeWeight;
	}
	
	public static double getTotalNodeDensity(Collection<Node> nodes) {
		double totalNodeDensity = 0;
		for (Node node : nodes) {
			totalNodeDensity += node.getDensity();
		}
		return totalNodeDensity;
	}

	public static double getTotalEdgeWeight(SimpleWeightedGraph<Node, DefaultWeightedEdge> graph, Set<Node> nodes) {
		double totalEdgeWeight = 0;
		Set<DefaultWeightedEdge> edges = new HashSet<DefaultWeightedEdge>();

		for (Node v : nodes) {
			for (Node u : nodes) {
				if ((u != v) && graph.containsEdge(u, v)) {
					edges.add(graph.getEdge(u, v));
				}
			}
		}
		
		for(DefaultWeightedEdge e: edges) {
			totalEdgeWeight += graph.getEdgeWeight(e);
		}

		return totalEdgeWeight;
	}
}