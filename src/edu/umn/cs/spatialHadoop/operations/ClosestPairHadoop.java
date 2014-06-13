/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;

public class ClosestPairHadoop {
	private static final NullWritable Dummy = NullWritable.get();
	private static double sample_ratio = 0.01;
	private static int sample_count = 100000;
	private static int localMemory = 400000; // can be changed
	
	private static Vector<Point> sample = new Vector<Point>();
	
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

	static class MyPoint extends Point {
		public MyPoint(double x, double y) {
			this.x = x; this.y = y;
		}
		@Override
		public String toString() {
			return this.x + "," + this.y;
		}
	}
	/***
	 * [l, r] inclusive
	 * @param l
	 * @param r
	 * @return
	 */
	public static DistanceAndPair nearestNeighbor(Point[] a, Point[] tmp, int l, int r) {
		if (l >= r) return new DistanceAndPair(Double.MAX_VALUE, null, null);
		
		int mid = (l + r) >> 1;
		double medianX = a[mid].x;
		DistanceAndPair delta1 = nearestNeighbor(a, tmp, l, mid);
		DistanceAndPair delta2 = nearestNeighbor(a, tmp, mid + 1, r);
		DistanceAndPair delta = delta1.distance < delta2.distance ? delta1 : delta2;
		int i = l, j = mid + 1, k = l;
		
		while (i <= mid && j <= r)
			if (a[i].y < a[j].y) tmp[k++] = a[i++];
			else tmp[k++] = a[j++];
		while(i <= mid) tmp[k++] = a[i++];
		while(j <= r) tmp[k++] = a[j++];
		
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
	
	public static class Map0 extends MapReduceBase implements
	Mapper<CellInfo, ArrayWritable, IntWritable, Point> {
		private int find(Point p) {
			if (p.x <= sample.get(0).x) return 0;
			else if (p.x > sample.lastElement().x) return sample.size();
			int left = 0, right = sample.size() - 1; 
			// sample[left] < p <= sample[right]
			while(left + 1 < right) {
				int mid = (left + right) / 2;
				if (sample.get(mid).x < p.x) left = mid;
				else right = mid;
			}
			return right;
		}
		@Override
		public void map(CellInfo mbr, ArrayWritable arr, OutputCollector<IntWritable, Point> out,
				Reporter reporter) throws IOException {
			Shape[] a = (Shape[])arr.get();
			for (Shape s : a) {
				out.collect(new IntWritable(find((Point)s)), (Point)s);
			}
		}
	}
	
	public static class Reduce0 extends MapReduceBase implements
	Reducer<IntWritable, Point, NullWritable, MyPoint> {
		@Override
		public void reduce(IntWritable key, Iterator<Point> it,
				OutputCollector<NullWritable, MyPoint> out,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			double x1 = Long.MAX_VALUE, x2 = Long.MIN_VALUE;
			Vector<Point> points = new Vector<Point>();
			while(it.hasNext()) points.add(it.next().clone());
			
			Point[] a = points.toArray(new Point[points.size()]);
			Point[] tmp = new Point[a.length];
			for (Point p : a) {
				if (p.x < x1) x1 = p.x;
				if (p.x > x2) x2 = p.x;
			}
			
			Arrays.sort(a);
			DistanceAndPair delta = nearestNeighbor(a, tmp, 0, a.length - 1);
			for (int i=0; i<a.length; i++) {
				Point p = (Point)a[i];
				if (p.x <= x1 + delta.distance || p.x >= x2 - delta.distance || p == delta.pair.first || p == delta.pair.second) {
					out.collect(Dummy, new MyPoint(p.x, p.y));
				}
			}
		}
	}	
	
	public static class Map1 extends MapReduceBase implements
	Mapper<CellInfo, ArrayWritable, NullWritable, Point> {
		@Override
		public void map(CellInfo mbr, ArrayWritable arr, OutputCollector<NullWritable, Point> out,
				Reporter reporter) throws IOException {
			System.out.println("I am here!");
			Shape[] a = (Shape[])arr.get();
			for (Shape s : a) {
				out.collect(Dummy, (Point)s);
			}
		}
	}
	
	public static class Reduce1 extends MapReduceBase implements
	Reducer<NullWritable, Point, NullWritable, DistanceAndPair> {

		@Override
		public void reduce(NullWritable useless, Iterator<Point> it,
				OutputCollector<NullWritable, DistanceAndPair> out,
				Reporter reporter) throws IOException {
			
			Vector<Point> buffer = new Vector<Point>();
			while (it.hasNext()) buffer.add(it.next().clone());
			
			Point a[] = buffer.toArray(new Point[buffer.size()]);
			Point tmp[] = new Point[buffer.size()];
			Arrays.sort(a);
			
			DistanceAndPair delta = nearestNeighbor(a, tmp, 0, a.length - 1);
			out.collect(Dummy, delta);
		}
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
	public static <S extends Shape> void cloesetPair(Path file, OperationsParams params) throws IOException {
		// Try to get file MBR from the MBRs of blocks
		JobConf job = new JobConf(params, ClosestPairHadoop.class);

		Path outputPath;
		FileSystem outFs = FileSystem.get(job);
		do {
			outputPath = new Path(file.getName()+".closest_pair_"+(int)(Math.random()*1000000));
		} while (outFs.exists(outputPath));
		outFs.delete(outputPath, true);

		job.setJobName("ClosestPair");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Point.class);
		
		job.setMapperClass(Map0.class);
		job.setReducerClass(Reduce0.class);
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

		job.setInputFormat(ShapeArrayInputFormat.class);
//		job.setInputFormat(ShapeInputFormat.class);
		ShapeInputFormat.setInputPaths(job, file);
		
		job.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		// Submit the job
		JobClient.runJob(job);
		//////////////////////////////////////////////////////////////////////////
		
		System.out.println("Begin second round!");
		// 2nd Round
		job = new JobConf(params, ClosestPairHadoop.class);
		job.setJobName("Second Round");
		job.setOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Point.class);
		
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
		
		job.setInputFormat(ShapeArrayInputFormat.class);
//		job.setInputFormat(ShapeInputFormat.class);
		ShapeInputFormat.setInputPaths(job, outputPath);  // The previous output is the current input
		
		Path newPath = new Path(outputPath.getName() + "_result");
		job.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, newPath);
		
		JobClient.runJob(job);
	}

	public static void samplePoint(FileSystem fs, Path inputFile) throws IOException {
		Shape stockShape = new Point();
		
		ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
		      @Override
		      public void collect(Point value) {
		        sample.add(value.clone());
		      }
		};
		OperationsParams params = new OperationsParams();
		params.setFloat("ratio", (float) sample_ratio);
		params.setInt("count", sample_count);
		params.setClass("shape", stockShape.getClass(), TextSerializable.class);
		params.setClass("outshape", Point.class, TextSerializable.class);
		Sampler.sample(new Path[] {inputFile}, resultCollector, params);
		Collections.sort(sample);
	}
	
	private static void printUsage() {
		System.out.println("Finds the average area of all rectangles in an input file");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file>: (*) Path to input file");
		
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	  GenericOptionsParser parser = new GenericOptionsParser(args);
	  OperationsParams params = new OperationsParams(parser);
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
		params.setClass("shape", Point.class, Shape.class);
		samplePoint(fs, inputFile);
		final long fileSize = fs.getFileStatus(inputFile).getLen();
		long delta = (long) (1.0 * sample.size() / (1.0 * fileSize / localMemory));
		if (delta == 0) delta = 1;
		System.out.println("delta = " +delta);
		Vector<Point> axis = new Vector<Point>();
		for (int i=0; i<sample.size(); i+=delta)
		  axis.add(sample.get(i));
		sample = axis;

		System.out.println("Finish Sampling.");
		cloesetPair(inputFile, params);
	}
}

