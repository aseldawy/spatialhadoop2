package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

public class Insert {

	private static void printUsage() {
		System.out.println("Insert data from a file to another file with same type of shape");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		if (!params.checkInput() || (inputFiles.length != 2)) {
			printUsage();
			System.exit(1);
		}
		
		Path originalFile = inputFiles[0];
		Path newFile = inputFiles[1];
		System.out.println(originalFile);
		System.out.println(newFile);
	}

}
