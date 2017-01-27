package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;

public class FilePartitioner extends Partitioner {

	protected ArrayList<CellInfo> cells;
	
	public FilePartitioner() {
		// TODO Auto-generated constructor stub
		cells = new ArrayList<CellInfo>();
	}

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

	}

	/**
	 * Create this partitioner based on information from master file
	 * @param inPath
	 * @param params
	 * @throws IOException
	 */
	public void createFromMasterFile(Path inPath, OperationsParams params) throws IOException {
		this.cells = new ArrayList<CellInfo>();

		Job job = Job.getInstance(params);
		final Configuration conf = job.getConfiguration();
		final String sindex = conf.get("sindex");

		Path masterPath = new Path(inPath, "_master." + sindex);
		FileSystem inFs = inPath.getFileSystem(params);
		Text tempLine = new Text2();
		LineReader in = new LineReader(inFs.open(masterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			CellInfo tempCellInfo = new CellInfo();
			tempCellInfo.cellId = tempPartition.cellId;
			tempCellInfo.set(tempPartition.cellMBR);
			this.cells.add(tempCellInfo);
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
		CellInfo result = null;
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

}
