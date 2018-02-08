package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

public class RTreeReorganizer {

	public static void reorganizeGroup(Path path, ArrayList<ArrayList<Partition>> splitPartitionGroups,
			OperationsParams params) {

	}

	public static void reorganizePartition(Path path, ArrayList<Partition> splitPartitions, OperationsParams params) {

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 1)) {
			System.exit(1);
		}

		Path currentPath = inputFiles[0];
	}
}
