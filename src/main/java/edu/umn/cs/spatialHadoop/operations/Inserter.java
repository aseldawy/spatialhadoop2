package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.Indexer;

public class Inserter {

	private static void insertLocal(Path currentPath, Path insertPath, OperationsParams params) {

	}

	private static Job insertMapReduce(Path currentPath, Path insertPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(params, "Inserter");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Indexer.class);

		// Start the job
		if (conf.getBoolean("background", false)) {
			// Run in background
			job.submit();
		} else {
			job.waitForCompletion(conf.getBoolean("verbose", false));
		}
		return job;
	}

	private static void printUsage() {
		System.out.println("Insert data from a file to another file with same type of shape");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 2)) {
			printUsage();
			System.exit(1);
		}

		Path originalFile = inputFiles[0];
		Path newFile = inputFiles[1];
		System.out.println("Original file: " + originalFile);
		System.out.println("New file: " + newFile);
	}

}
