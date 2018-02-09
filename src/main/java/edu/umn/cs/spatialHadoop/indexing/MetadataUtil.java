package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.Text2;

public class MetadataUtil {
	
	final static byte[] NewLine = new byte[] { '\n' };
	
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
	
	public static void dumpToFile(ArrayList<Partition> partitions, Path path, String filename) throws IOException {
		Path dumpPath = new Path(path, filename);
		dumpToFile(partitions, dumpPath);
	}
	
	public static void dumpToFile(ArrayList<Partition> partitions, Path dumpPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(dumpPath)) {
			fs.delete(dumpPath);
		}
		
		OutputStream out = fs.create(dumpPath);
		
		for (Partition partition : partitions) {
			Text splitLine = new Text2();
			partition.toText(splitLine);
			out.write(splitLine.getBytes(), 0, splitLine.getLength());
			out.write(NewLine);
		}
		
		out.close();
	}
	
	public static void dumpToWKTFile(ArrayList<Partition> partitions, Path dumpPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(dumpPath)) {
			fs.delete(dumpPath);
		}
		
		PrintStream out = new PrintStream(fs.create(dumpPath));
		out.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		
		for (Partition partition : partitions) {
			out.println(partition.toWKT());
		}
		
		out.close();
	}
	
	public static boolean isContainedPartition(ArrayList<Partition> partitions, Partition p) {
		for(Partition partition: partitions) {
			if(partition.cellId == p.cellId) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isOverlapping(ArrayList<Partition> partitions, Partition p) {
		for (Partition partition : partitions) {
			if ((p.cellId != partition.cellId) && p.isIntersected(partition)) {
				return true;
			}
		}
		return false;
	}
	
	public static ArrayList<Partition> deduplicatePartitions(ArrayList<Partition> partitions) {
		ArrayList<Partition> deduplicatedPartitions = new ArrayList<Partition>();
		Set<Integer> cellIdsSet = new HashSet<Integer>();
		
		for(Partition p: partitions) {
			if(!cellIdsSet.contains(p.cellId)) {
				deduplicatedPartitions.add(p);
				cellIdsSet.add(p.cellId);
			}
		}
		
		return deduplicatedPartitions;
	}
	
	public static int getMaximumCellId(ArrayList<Partition> partitions) {
		int maxCellId = -1;
		for (Partition partition : partitions) {
			if (partition.cellId > maxCellId) {
				maxCellId = partition.cellId;
			}
		}
		return maxCellId;
	}
	
	public static ArrayList<Partition> removePartitions(ArrayList<Partition> partitions, ArrayList<Partition> partitionsToRemove) {
		ArrayList<Partition> tempPartitions = new ArrayList<Partition>();
		for(Partition p: partitions) {
			if(isContainedPartition(partitionsToRemove, p)) {
				tempPartitions.add(p);
			}
		}
		partitions.removeAll(tempPartitions);
		return partitions;
	}
	
	public static ArrayList<ArrayList<Partition>> groupByOverlappingPartitions(ArrayList<Partition> partitions) {
		ArrayList<ArrayList<Partition>> groups = new ArrayList<ArrayList<Partition>>();
		@SuppressWarnings("unchecked")
		ArrayList<Partition> tempPartitions = (ArrayList<Partition>) partitions.clone();

		while (tempPartitions.size() > 0) {
			ArrayList<Partition> group = new ArrayList<Partition>();
			group.add(tempPartitions.get(0));
			for (Partition p : tempPartitions) {
				if (isOverlapping(group, p)) {
					if(!isContainedPartition(group, p)) {
						group.add(p);
					}
				}
			}
			groups.add(group);
			tempPartitions.removeAll(group);
		}
		return groups;
	}
}
