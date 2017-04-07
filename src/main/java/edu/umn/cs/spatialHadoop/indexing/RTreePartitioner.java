package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

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

	private RTree<Integer, Geometry> tree;
	private ArrayList<CellInfo> cells = new ArrayList<CellInfo>();

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		String tempString = "";
		for(CellInfo cell: this.cells) {
			Text text = new Text();
			cell.toText(text);
			tempString += text.toString() + "\n";
		}
		out.writeUTF(tempString);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		String tempString = in.readUTF();
		String[] cellTexts = tempString.split("\n");
		for(String text: cellTexts) {
			CellInfo tempCellInfo = new CellInfo();
			tempCellInfo.fromText(new Text(text));
			this.cells.add(tempCellInfo);
		}
	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int capacity) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		List<Entry<Integer, Geometry>> entries = new ArrayList<Entry<Integer, Geometry>>();
		for (int i = 0; i < points.length; i++) {
			Point p = points[i];
			entries.add(new EntryDefault<Integer, Geometry>(i, Geometries.point(p.x, p.y)));
		}

		tree = RTree.star().maxChildren(capacity).create();
		tree = tree.add(entries);
		tree.visualize(1500, 360).save("test.png");
		
		// Get list of all leaf nodes
		Node<Integer, Geometry> node = tree.root().get();
		List<Rectangle> rects = getAllLeafs(node);
		cells = new ArrayList<CellInfo>();
		int cellId = 1;
		for(Rectangle r: rects) {
			cells.add(new CellInfo(cellId, r));
			cellId++;
		}
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub
		for(CellInfo cell: this.cells) {
			if(cell.isIntersected(shape)) {
				matcher.collect(cell.cellId);
			}
		}
	}

	@Override
	public int overlapPartition(Shape shape) {
		// TODO Auto-generated method stub
		for(CellInfo cell: this.cells) {
			if(cell.isIntersected(shape)) {
				return cell.cellId;
			}
		}
		return 0;
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
