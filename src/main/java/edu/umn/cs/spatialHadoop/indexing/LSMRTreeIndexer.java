package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class LSMRTreeIndexer {

	private static final int COMPACTION_MIN_COMPONENT = 3;
	private static final int COMPACTION_MAX_COMPONENT = 10;
	private static final double COMPACTION_RATIO = 1.0;
	private static final int BUFFER_LINES = 10000;
	private static final Log LOG = LogFactory.getLog(Indexer.class);

	static class RTreeComponent {
		private String name;
		private Double size;

		private RTreeComponent(String name, Double size) {
			this.setName(name);
			this.setSize(size);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Double getSize() {
			return size;
		}

		public void setSize(Double size) {
			this.size = size;
		}
	}

	// Read the input by buffer then put them to a temporary input
	private static void insertByBuffer(Path inputPath, Path outputPath, OperationsParams params) throws IOException,
			ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {
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
				if (count == BUFFER_LINES) {
					writer.close();
					out.close();
					// Build R-Tree index from temp file
					componentId++;
					Path componentPath = new Path(outputPath, String.format("%d", componentId));
					OperationsParams params2 = params;
					params2.set("sindex", "str");
					Rectangle tempInputMBR = FileMBR.fileMBR(tempInputPath, params);
					params2.set("mbr", String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2,
							tempInputMBR.y2));
					params2.setBoolean("local", true);
					Indexer.index(tempInputPath, componentPath, params2);
					ArrayList<RTreeComponent> componentsToMerge = checkPolicy(outputPath);
					if (componentsToMerge.size() > 0) {
						merge(outputPath, componentsToMerge, params);
					}

					fs = FileSystem.get(new Configuration());
					out = fs.create(tempInputPath, true);
					writer = new PrintWriter(out);
					count = 0;
				} else {
					writer.append("\n");
				}
			} else {
				writer.close();
				out.close();
			}
		} while (line != null);
		fs.close();
	}

	private static ArrayList<RTreeComponent> checkPolicy(Path path) throws IOException {
		ArrayList<RTreeComponent> componentsToMerge = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		ArrayList<RTreeComponent> rtreeComponents = getComponents(path);
		for (RTreeComponent component : rtreeComponents) {
			System.out.println("component " + component.name + " has size " + component.size);
		}
		Collections.sort(rtreeComponents, new Comparator<RTreeComponent>() {
			@Override
			public int compare(RTreeComponent o1, RTreeComponent o2) {
				return o2.getSize().compareTo(o1.getSize());
			}
		});
		System.out.println("sorted components:");

		for (int i = rtreeComponents.size() - 1; i >= 0; i--) {
			RTreeComponent currentComponent = rtreeComponents.get(i);
			double sum = 0;
			for (int j = i - 1; j >= 0; j--) {
				sum += rtreeComponents.get(j).getSize();
			}
			if (currentComponent.getSize() < COMPACTION_RATIO * sum) {
				for (int k = i; k >= 0; k--) {
					componentsToMerge.add(rtreeComponents.get(k));
				}
				break;
			}
		}

		return componentsToMerge;
	}

	private static ArrayList<RTreeComponent> getComponents(Path path) throws IOException {
		ArrayList<RTreeComponent> rtreeComponents = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HashSet<String> components = new HashSet<String>();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			String filePath = fileStatus.getPath().toString();
			String componentPath = filePath.substring(0, filePath.lastIndexOf("/"));
			String component = componentPath.substring(componentPath.lastIndexOf("/") + 1);
			components.add(component);
		}

		for (String component : components) {
			rtreeComponents.add(new RTreeComponent(component,
					new Double(fs.getContentSummary(new Path(path, component)).getSpaceConsumed())));
		}

		return rtreeComponents;
	}

	private static void merge(Path path, ArrayList<RTreeComponent> componentsToMerge, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		System.out.println("merging following components:");
		double maxsize = componentsToMerge.get(0).getSize();
		String maxsizeComponent = componentsToMerge.get(0).getName();
		for (RTreeComponent component : componentsToMerge) {
			if(component.getSize() > maxsize) {
				maxsizeComponent = component.getName();
			}
		}

		Path tempMergePath = new Path("./", "temp.merge");
		fs.mkdirs(tempMergePath);
		for (RTreeComponent component : componentsToMerge) {
			FileStatus[] dataFiles = fs.listStatus(new Path(path, component.getName()), new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return path.getName().contains("part-");
				}
			});
			for (FileStatus file : dataFiles) {
				// Move file to temp merge directory
				System.out.println(file.getPath());
				String filePath = file.getPath().toString();
				fs.rename(file.getPath(), new Path(tempMergePath,
						filePath.substring(filePath.lastIndexOf("/") + 1) + "-" + component.getName()));
			}
			fs.delete(new Path(path, component.getName()));
		}
		
		OperationsParams params2 = params;
		params2.set("sindex", "str");
		Rectangle tempInputMBR = FileMBR.fileMBR(tempMergePath, params);
		params2.set("mbr", String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2,
				tempInputMBR.y2));
		params2.setBoolean("local", true);
		Indexer.index(tempMergePath, new Path(path, maxsizeComponent), params2);
		
		fs.delete(tempMergePath);
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
		// checkPolicy(inputPath);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis " + (t2 - t1));
	}
}
