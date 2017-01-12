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

	protected ArrayList<Partition> partitions;

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

	/**
	 * Create this partitioner based on information from master file
	 * @param inPath
	 * @param outPath
	 * @param params
	 * @throws IOException
	 */
	public void createFromMasterFile(Path inPath, final Path outPath, OperationsParams params) throws IOException {
		this.partitions = new ArrayList<Partition>();

		Job job = Job.getInstance(params);
		final Configuration conf = job.getConfiguration();

		final String sindex = conf.get("sindex");

		Path masterPath = new Path(outPath, "_master." + sindex);
		FileSystem outFs = outPath.getFileSystem(params);
		Text tempLine = new Text2();
		Partition tempPartition = new Partition();
		LineReader in = new LineReader(outFs.open(masterPath));
		while (in.readLine(tempLine) > 0) {
			tempPartition.fromText(tempLine);
			this.partitions.add(tempPartition);
		}
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO Auto-generated method stub
		for(Partition p: this.partitions) {
			if(p.isIntersected(shape)) {
				matcher.collect(p.cellId);
			}
		}
	}

	@Override
	public int overlapPartition(Shape shape) {
		// TODO Auto-generated method stub
		for(Partition p: this.partitions) {
			if(p.isIntersected(shape)) {
				return p.cellId;
			}
		}
		return 0;
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		// TODO Auto-generated method stub
		Partition partition = null;
		for (Partition p : this.partitions) {
			if (p.cellId == partitionID) {
				partition = p;
				break;
			}
		}
		return partition;
	}

	@Override
	public CellInfo getPartitionAt(int index) {
		// TODO Auto-generated method stub
		return this.partitions.get(index);
	}

	@Override
	public int getPartitionCount() {
		// TODO Auto-generated method stub
		return this.partitions.size();
	}

}
