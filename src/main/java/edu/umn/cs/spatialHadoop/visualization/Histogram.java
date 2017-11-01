package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.FileMBR.FileMBRMapper;
import edu.umn.cs.spatialHadoop.operations.FileMBR.Reduce;

public class Histogram {
	/**Logger for FileMBR*/
	private static final Log LOG = LogFactory.getLog(Histogram.class);
	
	private static final String HistogramWidth = "hist.width";
	private static final String HistogramHeight = "hist.height";
	
	public static class HistogramMapper<S extends Shape>
    extends Mapper<Rectangle, Iterable<S>, NullWritable, GridHistogram>{
		
		private GridHistogram gridHistogram;
		
		private Text text;
		
		private Rectangle fileMBR;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			int width = conf.getInt(HistogramWidth, 1024);
			int height = conf.getInt(HistogramHeight, 1024);
			gridHistogram = new GridHistogram(width, height);
			text = new Text();
			fileMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
		}

		@Override
		protected void map(Rectangle partitionMBR, Iterable<S> shapes, Context context)
				throws IOException, InterruptedException {
			for (Shape shape : shapes) {
				Rectangle mbr = shape.getMBR();
				text.clear();
				shape.toText(text);
				int size = text.getLength();
				
				double centerx = (mbr.x1 + mbr.x2) / 2;
				double centery = (mbr.y1 + mbr.y2) / 2;
				int gridColumn = (int) ((centerx - fileMBR.x1) * gridHistogram.getWidth() / fileMBR.getWidth());
				int gridRow = (int) ((centery - fileMBR.y1) * gridHistogram.getHeight() / fileMBR.getHeight());
				gridHistogram.set(gridColumn, gridRow, size);
			}
			context.write(NullWritable.get(), gridHistogram);
		}
		
	}
	
	public static class HistogramReducer
	  extends Reducer<NullWritable, GridHistogram, NullWritable, GridHistogram> {
		
		private GridHistogram gridHistogram;
		
		@Override
		protected void setup(Reducer<NullWritable, GridHistogram, NullWritable, GridHistogram>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			int width = conf.getInt(HistogramWidth, 1024);
			int height = conf.getInt(HistogramHeight, 1024);
			gridHistogram = new GridHistogram(width, height);
		}
		
		@Override
		protected void reduce(NullWritable arg0, Iterable<GridHistogram> histogram,
				Reducer<NullWritable, GridHistogram, NullWritable, GridHistogram>.Context arg2)
				throws IOException, InterruptedException {
			for (GridHistogram h : histogram) {
				gridHistogram.merge(h);
			}
			arg2.write(NullWritable.get(), gridHistogram);
		}
		
	}

	public static void histogram(Path[] inputFiles, Path outputFile, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
		histogramMapReduce(inputFiles, outputFile, params);
	}


	private static Job histogramMapReduce(Path[] inputFiles, Path outputFile, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(params, "Histogram");

		job.setJarByClass(Histogram.class);
		job.setMapperClass(HistogramMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(GridHistogram.class);
	    job.setReducerClass(HistogramReducer.class);

	    Rectangle inputMBR = (Rectangle) params.getShape("mbr");
	    if (inputMBR == null) {
	    	inputMBR = FileMBR.fileMBR(inputFiles, params);
	    	OperationsParams.setShape(job.getConfiguration(), "mbr", inputMBR);
	    }
	    
	    job.setInputFormatClass(SpatialInputFormat3.class);
	    SpatialInputFormat3.setInputPaths(job, inputFiles);
	    job.setOutputFormatClass(BinaryOutputFormat.class);
	    BinaryOutputFormat.setOutputPath(job, outputFile);
	    job.setNumReduceTasks(1);
	    job.getConfiguration().setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
	    
	    // Start the job
	    if (params.getBoolean("background", false)) {
	      // Run in background
	      job.submit();
	    } else {
	      job.waitForCompletion(params.getBoolean("verbose", false));
	    }
	    return job;

	}


	private static void printUsage() {
		System.out.println("Computes the histogram of an input file");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file>: (*) Path to input file");
		System.out.println("shape:<input shape>: (*) Input file format");
		System.out.println("hist.width:<num of columns>: Number of columns in the histogram (1024)");
		System.out.println("hist.height:<num of rows>: Number of rows in the histogram (1024)");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		if (!params.checkInputOutput()) {
			printUsage();
			System.exit(1);
		}
		Path[] inputFiles = params.getInputPaths();
		Path outputFile = params.getOutputPath();

		if (params.getShape("shape") == null) {
			LOG.error("Input file format not specified");
			printUsage();
			return;
		}
		long t1 = System.currentTimeMillis();
		histogram(inputFiles, outputFile, params);
		long t2 = System.currentTimeMillis();

		System.out.println("Computed histogram in: "+(t2-t1)/1000.0+" seconds");
	}

}
