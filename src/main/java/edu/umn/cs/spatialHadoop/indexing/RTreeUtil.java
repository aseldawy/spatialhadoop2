package edu.umn.cs.spatialHadoop.indexing;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.EntryDefault;

public class RTreeUtil {
	
	public static Rectangle2D.Double[] RStartTreePartitioning(Point2D.Double[] points, int numPartitions) {
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();
		int capacity = (int) Math.ceil((double) points.length / (double) numPartitions);
		RTree<Integer, Geometry> tree = RTree.star().maxChildren(capacity).create();
		for (int i = 0; i < points.length; i++) {
			Point2D.Double p = points[i];
			entries.add(new EntryDefault<Integer, Geometry>(i, Geometries.point(p.x, p.y)));
		}
		tree = tree.add(entries);
		Node<Integer, Geometry> root = tree.root().get();
		List<Rectangle2D.Double> rects = getLeafs(root);
		return (java.awt.geom.Rectangle2D.Double[]) rects.toArray();
	}

	private static <T, S extends Geometry> List<Rectangle2D.Double> getLeafs(Node<T, S> node) {
		List<Rectangle2D.Double> leafRects = new ArrayList<Rectangle2D.Double>();
		if (node instanceof Leaf) {
			final Leaf<T, S> leaf = (Leaf<T, S>) node;
			Rectangle2D.Double rect = new Rectangle2D.Double(leaf.geometry().mbr().x1(), leaf.geometry().mbr().y1(),
					leaf.geometry().mbr().x2() - leaf.geometry().mbr().x1(), leaf.geometry().mbr().y2() - leaf.geometry().mbr().y1());
			leafRects.add(rect);
		} else {
			final NonLeaf<T, S> n = (NonLeaf<T, S>) node;
			for (int i = 0; i < n.count(); i++) {
				leafRects.addAll(getLeafs(n.child(i)));
			}
		}

		return leafRects;
	}
}
