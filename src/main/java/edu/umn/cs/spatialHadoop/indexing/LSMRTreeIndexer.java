package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class LSMRTreeIndexer {
	
	private static final int BUFFER_LINES = 100000;
	private static final Log LOG = LogFactory.getLog(Indexer.class);
	
	// Read the input by buffer then put them to a temporary input
		private static void insertByBuffer(Path inputPath, Path outputPath, OperationsParams params) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {
			FileSystem fs = FileSystem.get(new Configuration());
			Path tempInputPath = new Path("./", "temp.input");
			FSDataOutputStream out = fs.create(tempInputPath, true);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String line;
			PrintWriter writer = new PrintWriter(out);
			int count = 0;
			int componentId = 0;

			do {
				line = br.readLine();
				if (line != null) {
					count++;
					writer.append(line);
					if(count == BUFFER_LINES) {
						writer.close();
						out.close();
						// Build R-Tree index from temp file
						componentId++;
						Path componentPath = new Path(outputPath, String.format("%d", componentId));
						OperationsParams params2 = params;
						params2.set("sindex", "rtree");
						Rectangle tempInputMBR = FileMBR.fileMBR(tempInputPath, params);
						params2.set("mbr", String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2, tempInputMBR.y2));
						params2.setBoolean("local", false);
						Indexer.index(tempInputPath, componentPath, params2);
						
						fs = FileSystem.get(new Configuration());
						out = fs.create(tempInputPath, true);
						writer = new PrintWriter(out);
						count = 0;
					}
					else {
						writer.append("\n");
					}
				} 
				else {
					writer.close();
					out.close();
				}
			} while (line != null);
			fs.close();
		}
	
	private static void printUsage() {
		System.out.println("Index data using LSM with R-Tree component");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		if (!params.checkInputOutput(true)) {
			printUsage();
			return;
		}

		Path inputPath = params.getInputPath();
		Path outputPath = params.getOutputPath();

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		insertByBuffer(inputPath, outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis " + (t2 - t1));
	}
}
