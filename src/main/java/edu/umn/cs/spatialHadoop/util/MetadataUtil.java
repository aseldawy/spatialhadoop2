package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.io.Text2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
		FileSystem fs = path.getFileSystem(params);
		Path masterPath = fs.listStatus(path, SpatialSite.MasterFileFilter)[0].getPath();

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
	
	public static boolean isContainedPartition(List<Partition> partitions, Partition p) {
		for(Partition partition: partitions) {
			if(partition.cellId == p.cellId) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isOverlapping(List<Partition> partitions, Partition p) {
		for (Partition partition : partitions) {
			if ((p.cellId != partition.cellId) && p.isIntersected(partition)) {
				return true;
			}
		}
		return false;
	}
	
	public static List<Partition> deduplicatePartitions(List<Partition> partitions) {
		List<Partition> deduplicatedPartitions = new ArrayList<Partition>();
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
	
	public static List<Partition> removePartitions(List<Partition> partitions, List<Partition> partitionsToRemove) {
		List<Partition> tempPartitions = new ArrayList<Partition>();
		for(Partition p: partitions) {
			if(isContainedPartition(partitionsToRemove, p)) {
				tempPartitions.add(p);
			}
		}
		partitions.removeAll(tempPartitions);
		return partitions;
	}
	
	public static List<List<Partition>> groupByOverlappingPartitions(List<Partition> partitions) {
		List<List<Partition>> groups = new ArrayList<List<Partition>>();
		@SuppressWarnings("unchecked")
		List<Partition> tempPartitions = new ArrayList(partitions);

		while (tempPartitions.size() > 0) {
			List<Partition> group = new ArrayList<Partition>();
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
	
	public static void printPartitionSummary(ArrayList<Partition> partitions, double blockSize) {
		long totalSplitSize = 0;
		int totalSplitBlocks = 0;

		for (Partition partition : partitions) {
			totalSplitSize += partition.size;
			totalSplitBlocks += partition.getNumberOfBlock(blockSize);
		}

		System.out.println("Total split partitions = " + partitions.size());
		System.out.println("Total split size = " + totalSplitSize);
		System.out.println("Total split blocks = " + totalSplitBlocks);
	}
	
	public static void printGroupSummary(ArrayList<ArrayList<Partition>> groups, double blockSize) {
		ArrayList<Partition> partitions = new ArrayList<Partition>();
		for(ArrayList<Partition> group: groups) {
			partitions.addAll(group);
		}
		printPartitionSummary(partitions, blockSize);
	}
}
