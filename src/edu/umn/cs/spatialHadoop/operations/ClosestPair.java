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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

public class ClosestPair {
  
  private static final Log LOG = LogFactory.getLog(ClosestPair.class);
  
	private static final NullWritable Dummy = NullWritable.get();
	
	static class DistanceAndPair implements Writable {
		double distance;
		PairWritable<Point> pair = new PairWritable<Point>();
		public DistanceAndPair() {
		}
		
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

	/***
	 * [l, r] inclusive
	 * @param l
	 * @param r
	 * @return
	 */
	public static DistanceAndPair nearestNeighbor(Shape[] a, Point[] tmp, int l, int r) {
		if (l >= r) return null;
		
		int mid = (l + r) >> 1;
		double medianX = ((Point)a[mid]).x;
		DistanceAndPair delta1 = nearestNeighbor(a, tmp, l, mid);
		DistanceAndPair delta2 = nearestNeighbor(a, tmp, mid + 1, r);
		DistanceAndPair delta;
		if (delta1 == null || delta2 == null) {
		  delta = delta1 == null ? delta2 : delta1;
		} else {
		  delta = delta1.distance < delta2.distance ? delta1 : delta2;
		}
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
			if (delta == null || Math.abs(tmp[i].x - medianX) <= delta.distance)
				tmp[k++] = tmp[i];

		for (i=l; i<k; i++)
			for (j=i+1; j<k; j++) {
				if (delta != null && tmp[j].y - tmp[i].y >= delta.distance) break;
				else if (delta == null || tmp[i].distanceTo(tmp[j]) < delta.distance) {
				  if (delta == null)
				    delta = new DistanceAndPair();
					delta.distance = tmp[i].distanceTo(tmp[j]);
					delta.pair.first = tmp[i];
					delta.pair.second = tmp[j];
				}
			}
		return delta;
	}
	
	public static class Map extends MapReduceBase implements
	Mapper<Rectangle, ArrayWritable, NullWritable, Point> {
		@Override
		public void map(Rectangle mbr, ArrayWritable arr, OutputCollector<NullWritable, Point> out,
				Reporter reporter) throws IOException {
			Shape[] a = (Shape[])arr.get();
			
			if (a.length == 1) {
			  // Cannot compute closest pair for an input of size 1
			  out.collect(Dummy, (Point) a[0]);
			  return;
			}
			
			Point[] tmp = new Point[a.length];
			Arrays.sort(a);
			DistanceAndPair delta = nearestNeighbor(a, tmp, 0, a.length - 1);
			if (delta.pair == null || delta.pair.first == null || delta.pair.second == null) {
			  LOG.error("Error null closest pair for input of size: "+a.length);
			}
			LOG.info("Found the closest pair: "+delta);
			
			Rectangle pruned_area = new Rectangle(mbr.x1 + delta.distance,
			    mbr.y1 + delta.distance,
			    mbr.x2 - delta.distance, mbr.y2 - delta.distance);
			
			LOG.info("MRR: "+mbr);
			LOG.info("Pruned area: "+pruned_area);

      out.collect(Dummy, delta.pair.first);
      out.collect(Dummy, delta.pair.second);
      
      int pruned_points = 0;
      
			for (int i=0; i < a.length; i++) {
				Point p = (Point)a[i];
				if (!pruned_area.contains(p) && p != delta.pair.first && p != delta.pair.second) {
				  out.collect(Dummy, p);
				} else {
				  pruned_points++;
				}
			}
			
			LOG.info("Total pruned points "+pruned_points);
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements
	Reducer<NullWritable, Point, NullWritable, DistanceAndPair> {

		@Override
		public void reduce(NullWritable useless, Iterator<Point> it,
				OutputCollector<NullWritable, DistanceAndPair> out,
				Reporter reporter) throws IOException {
		  
		  ArrayList<Point> allPoints = new ArrayList<Point>();
		  while (it.hasNext()) {
		    allPoints.add(it.next());
		  }
		  
		  Point[] all_points = allPoints.toArray(new Point[allPoints.size()]);
			
      DistanceAndPair delta = nearestNeighbor(all_points,
          new Point[all_points.length], 0, all_points.length - 1);
      out.collect(Dummy, delta);
		}
	}
	
  /**
   * Computes the closest pair by reading points from stream
   * @param p 
   * @throws IOException 
   */
  public static <S extends Point> void closestPairStream(S p) throws IOException {
    ShapeRecordReader<S> reader =
        new ShapeRecordReader<S>(System.in, 0, Long.MAX_VALUE);
    ArrayList<Point> points = new ArrayList<Point>();
    
    Rectangle key = new Rectangle();
    while (reader.next(key, p)) {
      points.add(p.clone());
    }
    Shape[] allPoints = points.toArray(new Shape[points.size()]);
    nearestNeighbor(allPoints, new Point[allPoints.length], 0, allPoints.length-1);
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
	public static void closestPair( Path file, OperationsParams params) throws IOException {
		// Try to get file MBR from the MBRs of blocks
	  Shape shape = params.getShape("shape");
		JobConf job = new JobConf(params, ClosestPair.class);

		Path outputPath;
		FileSystem outFs = FileSystem.get(job);
		do {
			outputPath = new Path(file.getName()+".closest_pair_"+(int)(Math.random()*1000000));
		} while (outFs.exists(outputPath));
		outFs.delete(outputPath, true);

		job.setJobName("ClosestPair");
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

		job.setInputFormat(ShapeArrayInputFormat.class);
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
		
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	  GenericOptionsParser parser = new GenericOptionsParser(args);
	  OperationsParams params = new OperationsParams(parser);
	  if (params.getPaths().length == 0 && params.is("local")) {
	    long t1 = System.currentTimeMillis();
	    closestPairStream((Point)params.getShape("shape"));
      long t2 = System.currentTimeMillis();
      System.out.println("Total time: "+(t2-t1)+" millis");
	    return;
	  }
		if (args.length == 0) {
			printUsage();
			throw new RuntimeException("Illegal arguments. Input file missing");
		}
		Path inputFile = new Path(args[0]);
		FileSystem fs = inputFile.getFileSystem(params);
		if (!fs.exists(inputFile)) {
			printUsage();
			throw new RuntimeException("Input file does not exist");
		}

		long t1 = System.currentTimeMillis();
		if (SpatialSite.getGlobalIndex(fs, inputFile) != null)
		  closestPair(inputFile, params);
		else
		  ClosestPairHadoop.cloesetPair(inputFile, params);
//		closestPairLocal(fs, inputFile, stockShape);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time: "+(t2-t1)+" millis");
	}
}
