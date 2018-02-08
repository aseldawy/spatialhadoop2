package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.Text2;

public class MetadataUtil {
	public static ArrayList<Partition> getPartitions(Path masterPath) throws IOException {

		ArrayList<Partition> partitions = new ArrayList<Partition>();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Text tempLine = new Text2();
		@SuppressWarnings("resource")
		LineReader in = new LineReader(fs.open(masterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			partitions.add(tempPartition);
		}

		return partitions;
	}

	public static ArrayList<Partition> getPartitions(Path path, OperationsParams params) throws IOException {
		
		String sindex = params.get("sindex");
		Path masterPath = new Path(path, "_master." + sindex);

		return getPartitions(masterPath);
	}
}
