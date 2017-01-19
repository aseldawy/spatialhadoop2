package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.Text2;

public class DynamicRepartitioner {
	
	private static void getPotentialPartitions(Path inPath, OperationsParams params) throws IOException {
		ArrayList<Partition> originalStandardPartitions = new ArrayList<Partition>();
		ArrayList<Partition> currentStandardPartitions = new ArrayList<Partition>();
		
		// Load original standard partitions
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");
		
		Path originalMasterPath = new Path(inPath, "_master." + sindex + ".std");
		Text tempLine = new Text2();
		LineReader in = new LineReader(fs.open(originalMasterPath));
		while (in.readLine(tempLine) > 0) {
			Partition tempPartition = new Partition();
			tempPartition.fromText(tempLine);
			originalStandardPartitions.add(tempPartition);
		}
		
		// Sampling to get current standard partitions
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Dynamic repartitioner");
	}

}
