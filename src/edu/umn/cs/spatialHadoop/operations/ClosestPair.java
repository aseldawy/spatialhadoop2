package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

public class ClosestPair {
	private static final NullWritable Dummy = NullWritable.get();
	
	static class DistanceAndPair implements Writable {
		double distance;
		PairWritable<Point> pair = new PairWritable<Point>();
		public DistanceAndPair() { }
		public DistanceAndPair(double d, Point a, Point b) {
			distance = d;
			pair.first = a;
			pair.second = b;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			distance = in.readDouble();
			pair.first = new Point(); 
			pair.second = new Point();
			pair.readFields(in);
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(distance);
			pair.write(out);
		}
		@Override 
		public String toString() {
			StringBuffer str = new StringBuffer();
			str.append(distance);    str.append("\n");
			str.append(pair.first);  str.append("\n");
			str.append(pair.second); str.append("\n");
			return str.toString();
		}
	}

	static class DistanceAndBuffer implements Writable {
		DistanceAndPair delta;
		Point []buffer;
		    
		public DistanceAndBuffer() { }
		public DistanceAndBuffer(DistanceAndPair delta, Point buffer[]) {
			this.delta = delta;
			this.buffer = buffer;
		}
		
	    @Override
	    public void write(DataOutput out) throws IOException {
	    	delta.write(out);
	    	out.writeInt(buffer.length);
	    	for (Point p : buffer)
	    		p.write(out);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	delta = new DistanceAndPair();
	    	delta.readFields(in);
	    	buffer = new Point[in.readInt()];
	    	for (int i=0; i<buffer.length; i++) {
	    		buffer[i] = new Point();
	    		buffer[i].readFields(in);
	    	}
	    }
	    
	    @Override
	    public String toString() {
	    	return "";
	    }
	}

	/***
	 * [l, r] inclusive
	 * @param l
	 * @param r
	 * @return
	 */
	public static DistanceAndPair nearestNeighbor(Shape[] a, Point[] tmp, int l, int r) {
		if (l >= r) return new DistanceAndPair(Double.MAX_VALUE, null, null);
		
		int mid = (l + r) >> 1;
		double medianX = ((Point)a[mid]).x;
		DistanceAndPair delta1 = nearestNeighbor(a, tmp, l, mid);
		DistanceAndPair delta2 = nearestNeighbor(a, tmp, mid + 1, r);
		DistanceAndPair delta = delta1.distance < delta2.distance ? delta1 : delta2;
		int i = l, j = mid + 1, k = l;
		
		while (i <= mid && j <= r)
			if (((Point)a[i]).y < ((Point)a[j]).y) tmp[k++] = (Point)a[i++];
			else tmp[k++] = (Point)a[j++];
		while(i <= mid) tmp[k++] = (Point)a[i++];
		while(j <= r) tmp[k++] = (Point)a[j++];
		
		for (i=l; i<=r; i++)
			a[i] = tmp[i];
		
		k = l;
		for (i=l; i<=r; i++)
			if (Math.abs(tmp[i].x - medianX) <= delta.distance)
				tmp[k++] = tmp[i];

		for (i=l; i<k; i++)
			for (j=i+1; j<k; j++) {
				if (tmp[j].y - tmp[i].y >= delta.distance) break;
				else if (tmp[i].distanceTo(tmp[j]) < delta.distance) {
					delta.distance = tmp[i].distanceTo(tmp[j]);
					delta.pair.first = tmp[i];
					delta.pair.second = tmp[j];
				}
			}
		return delta;
	}
	
	public static class Map extends MapReduceBase implements
	Mapper<CellInfo, ArrayWritable, NullWritable, DistanceAndBuffer> {
		@Override
		public void map(CellInfo mbr, ArrayWritable arr, OutputCollector<NullWritable, DistanceAndBuffer> out,
				Reporter reporter) throws IOException {
			double x1 = Long.MAX_VALUE, y1 = Long.MAX_VALUE;
			double x2 = Long.MIN_VALUE, y2 = Long.MIN_VALUE;
			Shape[] a = (Shape[])arr.get();
			Point[] tmp = new Point[a.length];
			boolean inBuffer[] = new boolean[a.length];
			for (Shape _p : a) {
				Point p = (Point)_p;
				if (p.x < x1) x1 = p.x;
				if (p.y < y1) y1 = p.y;
				if (p.x > x2) x2 = p.x;
				if (p.y > y2) y2 = p.y;
			}
			
			Arrays.sort(a);
			DistanceAndPair delta = nearestNeighbor(a, tmp, 0, a.length - 1);
			int cnt = 0;
			for (int i=0; i<a.length; i++) {
				Point p = (Point)a[i];
				if (p.x <= x1 + delta.distance || p.x >= x2 - delta.distance || 
					p.y <= y1 + delta.distance || p.y >= y2 - delta.distance) {
					inBuffer[i] = true;
					cnt++;
				}
			}
			
			Point buffer[] = new Point[cnt];
			int ptr = 0;
			for (int i=0; i<a.length; i++)
				if (inBuffer[i]) buffer[ptr++] = (Point)a[i];
			
			out.collect(Dummy, new DistanceAndBuffer(delta, buffer));
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements
	Reducer<NullWritable, DistanceAndBuffer, NullWritable, DistanceAndPair> {

		@Override
		public void reduce(NullWritable useless, Iterator<DistanceAndBuffer> it,
				OutputCollector<NullWritable, DistanceAndPair> out,
				Reporter reporter) throws IOException {
			
			DistanceAndPair delta = null;
			LinkedList<Point> buffer = new LinkedList<Point>();
			while (it.hasNext()) {
				DistanceAndBuffer localBuffer = it.next();
				if (delta == null) delta = localBuffer.delta;
				else if (localBuffer.delta.distance < delta.distance) delta = localBuffer.delta;
				
				for (Point p : localBuffer.buffer) 
					buffer.add(p);
			}
			
			Point a[] = new Point[buffer.size()];
			Point tmp[] = new Point[buffer.size()];
			a = buffer.toArray(a);
			Arrays.sort(a);
			DistanceAndPair delta1 = nearestNeighbor(a, tmp, 0, a.length - 1);
			
			if (delta == null || delta1.distance < delta.distance) delta = delta1;
			out.collect(Dummy, delta);
		}
	}
	
	public static <S extends Shape> void closestPairLocal(FileSystem fs,
			Path file, S stockShape) throws IOException {
		ShapeRecordReader<S> reader = new ShapeRecordReader<S>(fs.open(file), 0, fs.getFileStatus(file).getLen());
		Rectangle rect = new Rectangle();
		ArrayList<Shape> cb = new ArrayList<Shape>();
		while (reader.next(rect, stockShape)){
			cb.add(stockShape.clone());
		}
		System.out.println("here");
		Shape []a = cb.toArray(new Point[cb.size()]);
		Point []tmp = new Point[cb.size()];
		Arrays.sort(a);
		System.out.println("preprocessing is done.");
		
		DistanceAndPair delta = nearestNeighbor(a, tmp, 0, a.length - 1);
		System.out.println(delta);
	}
	
	/**
	 * Counts the exact number of lines in a file by issuing a MapReduce job
	 * that does the thing
	 * @param conf
	 * @param fs
	 * @param file
	 * @return
	 * @throws IOException 
	 */
	public static <S extends Shape> void closestPair(FileSystem fs,
			Path file, S stockShape) throws IOException {
		// Try to get file MBR from the MBRs of blocks
		JobConf job = new JobConf(ClosestPair.class);

		Path outputPath;
		FileSystem outFs = FileSystem.get(job);
		do {
			outputPath = new Path(file.getName()+".closest_pair_"+(int)(Math.random()*1000000));
		} while (outFs.exists(outputPath));
		outFs.delete(outputPath, true);

		job.setJobName("ClosestPair");
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DistanceAndBuffer.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

		job.setInputFormat(ShapeArrayInputFormat.class);
//		job.setInputFormat(ShapeInputFormat.class);
		SpatialSite.setShapeClass(job, stockShape.getClass());
		ShapeInputFormat.setInputPaths(job, file);

		job.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputPath);

		// Submit the job
		JobClient.runJob(job);
		
    outFs.delete(outputPath, true);
	}

	private static void printUsage() {
		System.out.println("Finds the average area of all rectangles in an input file");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file>: (*) Path to input file");
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			printUsage();
			throw new RuntimeException("Illegal arguments. Input file missing");
		}
		Path inputFile = new Path(args[0]);
		FileSystem fs = inputFile.getFileSystem(new Configuration());
		if (!fs.exists(inputFile)) {
			printUsage();
			throw new RuntimeException("Input file does not exist");
		}

		Shape stockShape = new Point();
		closestPair(fs, inputFile, stockShape);
//		closestPairLocal(fs, inputFile, stockShape);
	}
}
