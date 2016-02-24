/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.Progressable;

/**
 * Performs simple algorithms for spatial data.
 * 
 * @author Ahmed Eldawy
 * 
 */
class RectangleNN implements Comparable<RectangleNN>   {
	Rectangle r;
	float dist;
	public RectangleNN(Rectangle r, float dist){
		this.r =r ;
		this.dist =dist;	   
	}

	public int compareTo(RectangleNN rect2) {
		float difference = this.dist - rect2.dist;
		if (difference < 0) {
			return -1;
		} 
		if (difference > 0) {
			return 1;
		}
		return 0;

	}

}
class TOPK {
	public TreeSet<RectangleNN> heap;
	public int k;

	public TOPK(int k) {
		heap = new TreeSet<RectangleNN>();
		this.k = k;
	}

	public void add(Rectangle r,float dist) {
		heap.add(new RectangleNN(r, dist));
		if (this.heap.size() > k) {
			// Remove largest element in set (to keep it of size k)
			this.heap.last();
		}

	}
}

public class SpatialAlgorithms {
  public static final Log LOG = LogFactory.getLog(SpatialAlgorithms.class);

  
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweepFilterOnly(
	      final List<S1> R, final List<S2> S, final ResultCollector2<S1, S2> output,
	      Reporter reporter)
	      throws IOException {
	  
	  	LOG.info("Start spatial join plan sweep algorithm !!!");
	  
	    final RectangleID[] Rmbrs = new RectangleID[R.size()];
	    for (int i = 0; i < R.size(); i++) {
	      Rmbrs[i] = new RectangleID(i, R.get(i).getMBR());
	    }
	    final RectangleID[] Smbrs = new RectangleID[S.size()];
	    for (int i = 0; i < S.size(); i++) {
	      Smbrs[i] = new RectangleID(i, S.get(i).getMBR());
	    }	    
	    
	    final IntWritable count = new IntWritable();
	    int filterCount = SpatialJoin_rectangles(Rmbrs, Smbrs, new OutputCollector<RectangleID, RectangleID>() {
	        @Override
	        public void collect(RectangleID r1, RectangleID r2)
	            throws IOException {
	          //if (R.get(r1.id).isIntersected(S.get(r2.id))) {
	            if (output != null)
	              output.collect(R.get(r1.id), S.get(r2.id));
	            count.set(count.get() + 1);
	          //}
	        }
	    }, reporter);
	      
	      LOG.info("Filtered result size "+filterCount+", refined result size "+count.get());
	      
	      return count.get();
	}

  
  /**
   * @param R
   * @param S
   * @param output
   * @return
   * @throws IOException
   */
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      List<S1> R, List<S2> S, ResultCollector2<S1, S2> output, Reporter reporter)
      throws IOException {
    int count = 0;

    Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
    	if (o1.getMBR().x1 == o2.getMBR().x1)
    		return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    long t1 = System.currentTimeMillis();
    LOG.info("Joining lists "+ R.size()+" with "+S.size());
    Collections.sort(R, comparator);
    Collections.sort(S, comparator);

		int i = 0, j = 0;

    try {
      while (i < R.size() && j < S.size()) {
        S1 r;
        S2 s;
        if (comparator.compare(R.get(i), S.get(j)) < 0) {
          r = R.get(i);
          int jj = j;

          while ((jj < S.size())
              && ((s = S.get(jj)).getMBR().x1 <= r.getMBR().x2)) {
            // Check if r and s are overlapping but not the same object
            // for self join
            if (r.isIntersected(s) && !r.equals(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            jj++;
            if (reporter !=  null)
              reporter.progress();
          }
          i++;
        } else {
          s = S.get(j);
          int ii = i;

          while ((ii < R.size())
              && ((r = R.get(ii)).getMBR().x1 <= s.getMBR().x2)) {
            if (r.isIntersected(s) && !r.equals(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            ii++;
            if (reporter !=  null)
              reporter.progress();
          }
          j++;
        }
        if (reporter !=  null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info("Finished plane sweep in "+(t2-t1)+" millis and found "+count+" pairs");
    return count;
	}

  
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweepFilterOnly(
	      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output, Reporter reporter) {
	    int count = 0;

	    final Comparator<Shape> comparator = new Comparator<Shape>() {
	      @Override
	      public int compare(Shape o1, Shape o2) {
	    	if (o1.getMBR().x1 == o2.getMBR().x1)
	    		return 0;
	        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
	      }
	    };
	    
	    long t1 = System.currentTimeMillis();
	    LOG.info("Joining arrays "+ R.length+" with "+S.length);
	    Arrays.sort(R, comparator);
	    Arrays.sort(S, comparator);

	    int i = 0, j = 0;

	    try {
	      while (i < R.length && j < S.length) {
	        S1 r;
	        S2 s;
	        if (comparator.compare(R[i], S[j]) < 0) {
	          r = R[i];
	          int jj = j;

	          while ((jj < S.length)
	              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
	            if (r.getMBR().isIntersected(s.getMBR())) {
	              if (output != null)
	                output.collect(r, s);
	              count++;
	            }
	            jj++;
	            
	            if (reporter != null)
	              reporter.progress();
	          }
	          i++;
	        } else {
	          s = S[j];
	          int ii = i;

	          while ((ii < R.length)
	              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
	            if (r.getMBR().isIntersected(s.getMBR())) {
	              if (output != null)
	                output.collect(r, s);
	              count++;
	            }
	            ii++;
	          }
	          j++;
	          if (reporter != null)
	            reporter.progress();
	        }
	        if (reporter != null)
	          reporter.progress();
	      }
	    } catch (RuntimeException e) {
	      e.printStackTrace();
	    }
	    long t2 = System.currentTimeMillis();
	    LOG.info("Finished plane sweep filter only in "+(t2-t1)+" millis and found "+count+" pairs");
	    return count;
	  }

  
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output, Reporter reporter) {
    int count = 0;

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
    	if (o1.getMBR().x1 == o2.getMBR().x1)
    		return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };
    
    long t1 = System.currentTimeMillis();
    LOG.info("Joining arrays "+ R.length+" with "+S.length);
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);

    int i = 0, j = 0;

    try {
      while (i < R.length && j < S.length) {
        S1 r;
        S2 s;
        if (comparator.compare(R[i], S[j]) < 0) {
          r = R[i];
          int jj = j;

          while ((jj < S.length)
              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
            if (r.isIntersected(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            jj++;
            if (reporter != null)
              reporter.progress();
          }
          i++;
        } else {
          s = S[j];
          int ii = i;

          while ((ii < R.length)
              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
            if (r.isIntersected(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            ii++;
            if (reporter != null)
              reporter.progress();
          }
          j++;
        }
        if (reporter != null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info("Finished plane sweep in "+(t2-t1)+" millis and found "+count+" pairs");
    return count;
  }

  /**
   * Self join of rectangles. This method runs faster than the general version
   * because it just performs the filter step based on the rectangles.
   * @param output
   * @return
   * @throws IOException
   */
  public static <S1 extends Rectangle, S2 extends Rectangle> int SpatialJoin_rectangles(final S1[] R, final S2[] S,
      OutputCollector<S1, S2> output, Reporter reporter) throws IOException {
    int count = 0;

    final Comparator<Rectangle> comparator = new Comparator<Rectangle>() {
      @Override
      public int compare(Rectangle o1, Rectangle o2) {
    	if (o1.x1 == o2.x1)
    		  return 0;
        return o1.x1 < o2.x1 ? -1 : 1;
      }
    };
    
    long t1 = System.currentTimeMillis();
    LOG.info("Spatial Join of "+ R.length+" X " + S.length + "shapes");
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);
    
    int i = 0, j = 0;

    try {
    	 while (i < R.length && j < S.length) {
    	        S1 r;
    	        S2 s;
    	        if (comparator.compare(R[i], S[j]) < 0) {
    	          r = R[i];
    	          int jj = j;

    	          while ((jj < S.length)
    	              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
    	            if (r.isIntersected(s)) {
    	              if (output != null)
    	                output.collect(r, s);
    	              count++;
    	            }
    	            jj++;
    	          }
    	          i++;
    	          if (reporter != null)
    	            reporter.progress();
    	        } else {
    	          s = S[j];
    	          int ii = i;

    	          while ((ii < R.length)
    	              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
    	            if (r.isIntersected(s)) {
    	              if (output != null)
    	                output.collect(r, s);
    	              count++;
    	            }
    	            ii++;
    	            if (reporter != null)
    	              reporter.progress();
    	          }
    	          j++;
    	        }
    	        if (reporter != null)
    	          reporter.progress();
    	      }

    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info("Finished spatial join plane sweep in "+(t2-t1)+" millis and found "+count+" pairs");
    
    return count;
  }

  
  /**
   * Self join of rectangles. This method runs faster than the general version
   * because it just performs the filter step based on the rectangles.
   * @param rs
   * @param output
   * @return
   * @throws IOException
   */
  public static <S extends Rectangle> int SelfJoin_rectangles(final S[] rs,
      OutputCollector<S, S> output, Progressable reporter) throws IOException {
    int count = 0;

    final Comparator<Rectangle> comparator = new Comparator<Rectangle>() {
      @Override
      public int compare(Rectangle o1, Rectangle o2) {
    	if (o1.x1 == o2.x1)
    		  return 0;
        return o1.x1 < o2.x1 ? -1 : 1;
      }
    };
    
    long t1 = System.currentTimeMillis();
    LOG.info("Self Join of "+ rs.length+" shapes");
    Arrays.sort(rs, comparator);

    int i = 0, j = 0;

    try {
      while (i < rs.length && j < rs.length) {
        S r;
        S s;
        if (rs[i].x1 < rs[j].x1) {
          r = rs[i];
          int jj = j;

          while ((jj < rs.length)
              && ((s = rs[jj]).x1 <= r.x2)) {
            if (r != s && r.isIntersected(s)) {
              if (output != null) {
                output.collect(r, s);
              }
              count++;
            }
            jj++;
            if (reporter != null)
              reporter.progress();
          }
          i++;
        } else {
          s = rs[j];
          int ii = i;

          while ((ii < rs.length)
              && ((r = rs[ii]).x1 <= s.x2)) {
            if (r != s && r.isIntersected(s)) {
              if (output != null) {
                output.collect(r, s);
              }
              count++;
            }
            ii++;
            if (reporter != null)
              reporter.progress();
          }
          j++;
        }
        if (reporter != null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info("Finished self plane sweep in "+(t2-t1)+" millis and found "+count+" pairs");
    
    return count;
  }

  /**
   * MBR of a shape along with its ID. Used to performs the filter step while
   * keeping track of the ID of each object to be able to do the refine step.
   * @author Ahmed Eldawy
   *
   */
  public static class RectangleID extends Rectangle {
    public int id;
    
    public RectangleID(int id, Rectangle rect) {
      super(rect);
      this.id = id;
    }
    
    public RectangleID(int id, double x1, double y1, double x2, double y2) {
      super(x1, y1, x2, y2);
      this.id = id;
    }
  }
  
  /**
   * The general version of self join algorithm which works with arbitrary
   * shapes. First, it performs a filter step where it finds shapes with
   * overlapping MBRs. Second, an optional refine step can be executed to
   * return only shapes which actually overlap.
   * @param R - input set of shapes
   * @param refine - Whether or not to run a refine step
   * @param output - output collector where the results are reported
   * @return - number of pairs returned by the planesweep algorithm
   * @throws IOException
   */
  public static <S extends Shape> int SelfJoin_planeSweep(final S[] R,
      boolean refine, final OutputCollector<S, S> output, Progressable reporter) throws IOException {
    // Use a two-phase filter and refine approach
    // 1- Use MBRs as a first filter
    // 2- Use ConvexHull as a second filter
    // 3- Use the exact shape for refinement
    final RectangleID[] mbrs = new RectangleID[R.length];
    for (int i = 0; i < R.length; i++) {
      mbrs[i] = new RectangleID(i, R[i].getMBR());
    }
    
    if (refine) {
      final IntWritable count = new IntWritable();
      int filterCount = SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>() {
        @Override
        public void collect(RectangleID r1, RectangleID r2)
            throws IOException {
          if (R[r1.id].isIntersected(R[r2.id])) {
            if (output != null)
              output.collect(R[r1.id], R[r2.id]);
            count.set(count.get() + 1);
          }
        }
      }, reporter);
      
      LOG.info("Filtered result size "+filterCount+", refined result size "+count.get());
      
      return count.get();
    } else {
      return SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>() {
        @Override
        public void collect(RectangleID r1, RectangleID r2)
            throws IOException {
          if (output != null)
            output.collect(R[r1.id], R[r2.id]);
        }
      }, reporter);
    }
  }
  
  /**
   * Remove duplicate points from an array of points. Two points are considered
   * duplicate if both the horizontal and vertical distances are within a given
   * threshold distance.
   * @param allPoints
   * @param threshold
   * @return
   */
  public static Point[] deduplicatePoints(Point[] allPoints, final float threshold) {
    BitArray duplicates = new BitArray(allPoints.length);
    int numDuplicates = 0;
    LOG.info("Deduplicating a list of "+allPoints.length+" points");
    // Remove duplicates to ensure correctness
    Arrays.sort(allPoints, new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        double dx = p1.x - p2.x;
        if (dx < 0) return -1;
        if (dx > 0) return 1;
        double dy = p1.y - p2.y;
        if (dy < 0) return -1;
        if (dy > 0) return 1;
        return 0;
      }
    });

    for (int i = 1; i < allPoints.length; i++) {
      Point p1 = allPoints[i-1];
      Point p2 = allPoints[i];
      double dx = Math.abs(p1.x - p2.x);
      double dy = Math.abs(p1.y - p2.y);
      if (dx < threshold && dy < threshold) {
        duplicates.set(i, true);
        numDuplicates++;
      }
    }

    if (numDuplicates > 0) {
      LOG.info("Shrinking the array");
      // Shrinking the array
      Point[] newAllPoints = new Point[allPoints.length - numDuplicates];
      int newI = 0, oldI1 = 0;
      while (oldI1 < allPoints.length) {
        // Advance to the first non-duplicate point (start of range to be copied)
        while (oldI1 < allPoints.length && duplicates.get(oldI1))
          oldI1++;
        if (oldI1 < allPoints.length) {
          int oldI2 = oldI1 + 1;
          // Advance to the first duplicate point (end of range to be copied)
          while (oldI2 < allPoints.length && !duplicates.get(oldI2))
            oldI2++;
          // Copy the range [oldI1, oldI2[ to the new array
          System.arraycopy(allPoints, oldI1, newAllPoints, newI, oldI2 - oldI1);
          newI += (oldI2 - oldI1);
          oldI1 = oldI2;
        }
      }
      allPoints = newAllPoints;
    }
    return allPoints;
  }

  /**
   * Flatten geometries by extracting all internal geometries inside each
   * geometry.
   * @param geoms
   * @return
   */
  public static Geometry[] flattenGeometries(final Geometry[] geoms) {
    int flattenedNumberOfGeometries = 0;
    for (Geometry geom : geoms)
      flattenedNumberOfGeometries += geom.getNumGeometries();
    if (flattenedNumberOfGeometries == geoms.length)
      return geoms;
    Geometry[] flattenedGeometries = new Geometry[flattenedNumberOfGeometries];
    int i = 0;
    for (Geometry geom : geoms)
      for (int n = 0; n < geom.getNumGeometries(); n++)
        flattenedGeometries[i++] = geom.getGeometryN(n);
    return flattenedGeometries;
  }

  /**
   * Group polygons by overlap
   * @param polygons
   * @param prog
   * @return
   * @throws IOException
   */
  public static Geometry[][] groupPolygons(final Geometry[] polygons,
      final Progressable prog) throws IOException {
    // Group shapes into overlapping groups
    long t1 = System.currentTimeMillis();
    RectangleID[] mbrs = new RectangleID[polygons.length];
    for (int i = 0; i < polygons.length; i++) {
      Coordinate[] coords = polygons[i].getEnvelope().getCoordinates();
      double x1 = Math.min(coords[0].x, coords[2].x);
      double x2 = Math.max(coords[0].x, coords[2].x);
      double y1 = Math.min(coords[0].y, coords[2].y);
      double y2 = Math.max(coords[0].y, coords[2].y);
      
      mbrs[i] = new RectangleID(i, x1, y1, x2, y2);
    }
    
    // Parent link of the Set Union Find data structure
    final int[] parent = new int[mbrs.length];
    Arrays.fill(parent, -1);
    
    // Group records in clusters by overlapping
    SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>(){
      @Override
      public void collect(RectangleID r, RectangleID s)
          throws IOException {
        int rid = r.id;
        while (parent[rid] != -1) {
          int pid = parent[rid];
          if (parent[pid] != -1)
            parent[rid] = parent[pid];
          rid = pid;
        }
        int sid = s.id;
        while (parent[sid] != -1) {
          int pid = parent[sid];
          if (parent[pid] != -1)
            parent[sid] = parent[pid];
          sid = pid;
        }
        if (rid != sid)
          parent[rid] = sid;
      }}, prog);
    mbrs = null;
    // Put all records in one cluster as a list
    Map<Integer, List<Geometry>> groups = new HashMap<Integer, List<Geometry>>();
    for (int i = 0; i < parent.length; i++) {
      int root = parent[i];
      if (root == -1)
        root = i;
      while (parent[root] != -1) {
        root = parent[root];
      }
      List<Geometry> group = groups.get(root);
      if (group == null) {
        group = new Vector<Geometry>();
        groups.put(root, group);
      }
      group.add(polygons[i]);
    }
    long t2 = System.currentTimeMillis();
    
    Geometry[][] groupedPolygons = new Geometry[groups.size()][];
    int counter = 0;
    for (List<Geometry> group : groups.values()) {
      groupedPolygons[counter++] = group.toArray(new Geometry[group.size()]);
    }
    LOG.info("Grouped "+parent.length+" shapes into "+groups.size()+" clusters in "+(t2-t1)/1000.0+" seconds");
    return groupedPolygons;
  }


  /**
   * Directly unions the given list of polygons using a safe method that tries
   * to avoid geometry exceptions. First, it tries the buffer(0) method. It it
   * fails, it falls back to the tradition union method.
   * @param polys
   * @param progress
   * @return
   * @throws IOException 
   */
  public static Geometry safeUnion(List<Geometry> polys,
      Progressable progress) throws IOException {
    if (polys.size() == 1)
      return polys.get(0);
    Stack<Integer> rangeStarts = new Stack<Integer>();
    Stack<Integer> rangeEnds = new Stack<Integer>();
    rangeStarts.push(0);
    rangeEnds.push(polys.size());
    List<Geometry> results = new ArrayList<Geometry>();
    
    final GeometryFactory geomFactory = new GeometryFactory();
    
    // Minimum range size that is broken into two subranges
    final int MinimumThreshold = 10;
    // Progress numerator and denominator
    int progressNum = 0, progressDen = polys.size();
    
    while (!rangeStarts.isEmpty()) {
      int rangeStart = rangeStarts.pop();
      int rangeEnd = rangeEnds.pop();
      
      try {
        // Union using the buffer operation
        GeometryCollection rangeInOne = (GeometryCollection) geomFactory.buildGeometry(polys.subList(rangeStart, rangeEnd));
        Geometry rangeUnion = rangeInOne.buffer(0);
        results.add(rangeUnion);
        progressNum += rangeEnd - rangeStart;
      } catch (Exception e) {
        LOG.warn("Exception in merging "+(rangeEnd - rangeStart)+" polygons", e);
        // Fall back to the union operation
        if (rangeEnd - rangeStart < MinimumThreshold) {
          LOG.info("Error in union "+rangeStart+"-"+rangeEnd);
          // Do the union directly using the old method (union)
          Geometry rangeUnion = geomFactory.buildGeometry(new ArrayList<Geometry>());
          for (int i = rangeStart; i < rangeEnd; i++) {
            LOG.info(polys.get(i).toText());
          }
          for (int i = rangeStart; i < rangeEnd; i++) {
            try {
              rangeUnion = rangeUnion.union(polys.get(i));
            } catch (Exception e1) {
              // Log the error and skip it to allow the method to finish
              LOG.error("Error computing union", e);
            }
          }
          results.add(rangeUnion);
          progressNum += rangeEnd - rangeStart;
        } else {
          // Further break the range into two subranges
          rangeStarts.push(rangeStart);
          rangeEnds.push((rangeStart + rangeEnd) / 2);
          rangeStarts.push((rangeStart + rangeEnd) / 2);
          rangeEnds.push(rangeEnd);
          progressDen++;
        }
      }
      if (progress != null)
        progress.progress(progressNum/(float)progressDen);
    }
    
    // Finally, union all the results
    Geometry finalResult = results.remove(results.size() - 1);
    while (!results.isEmpty()) {
      try {
        finalResult = finalResult.union(results.remove(results.size() - 1));
      } catch (Exception e) {
        LOG.error("Error in union", e);
      }
      progressNum++;
      progress.progress(progressNum/(float)progressDen);
    }
    return finalResult;
  }


  /**
   * Union a group of (overlapping) geometries. It runs as follows.
   * <ol>
   *  <li>All polygons are sorted by the x-dimension of their left most point</li>
   *  <li>We run a plane-sweep algorithm that keeps merging polygons in batches of 500 objects</li>
   *  <li>As soon as part of the answer is to the left of the sweep-line, it
   *   is finalized and reported to the output</li>
   *  <li>As the sweep line reaches the far right, all remaining polygons are
   *   merged and the answer is reported</li>
   * </ol>
   * @param geoms
   * @return
   * @throws IOException 
   */
  public static int unionGroup(final Geometry[] geoms,
      final Progressable prog, ResultCollector<Geometry> output) throws IOException {
    if (geoms.length == 1) {
      output.collect(geoms[0]);
      return 1;
    }
    // Sort objects by x to increase the chance of merging overlapping objects
    for (Geometry geom : geoms) {
      Coordinate[] coords = geom.getEnvelope().getCoordinates();
      double minx = Math.min(coords[0].x, coords[2].x);
      geom.setUserData(minx);
    }
    
    Arrays.sort(geoms, new Comparator<Geometry>() {
      @Override
      public int compare(Geometry o1, Geometry o2) {
        Double d1 = (Double) o1.getUserData();
        Double d2 = (Double) o2.getUserData();
        if (d1 < d2) return -1;
        if (d1 > d2) return +1;
        return 0;
      }
    });
    LOG.info("Sorted "+geoms.length+" geometries by x");
  
    final int MaxBatchSize = 500;
    // All polygons that are to the right of the sweep line
    List<Geometry> nonFinalPolygons = new ArrayList<Geometry>();
    int resultSize = 0;
    
    long reportTime = 0;
    int i = 0;
    while (i < geoms.length) {
      int batchSize = Math.min(MaxBatchSize, geoms.length - i);
      for (int j = 0; j < batchSize; j++) {
        nonFinalPolygons.add(geoms[i++]);
      }
      double sweepLinePosition = (Double)nonFinalPolygons.get(nonFinalPolygons.size() - 1).getUserData();
      Geometry batchUnion;
      batchUnion = safeUnion(nonFinalPolygons, new Progressable.NullProgressable() {
        @Override
        public void progress() { prog.progress(); }
      });
      if (prog != null)
        prog.progress();
  
      nonFinalPolygons.clear();
      if (batchUnion instanceof GeometryCollection) {
        GeometryCollection coll = (GeometryCollection) batchUnion;
        for (int n = 0; n < coll.getNumGeometries(); n++) {
          Geometry geom = coll.getGeometryN(n);
          Coordinate[] coords = geom.getEnvelope().getCoordinates();
          double maxx = Math.max(coords[0].x, coords[2].x);
          if (maxx < sweepLinePosition) {
            // This part is finalized
            resultSize++;
            if (output != null)
              output.collect(geom);
          } else {
            nonFinalPolygons.add(geom);
          }
        }
      } else {
        nonFinalPolygons.add(batchUnion);
      }
  
      long currentTime = System.currentTimeMillis();
      if (currentTime - reportTime > 60*1000) { // Report every one minute
        if (prog != null) {
          float p = i / (float)geoms.length;
          prog.progress(p);
        }
        reportTime =  currentTime;
      }
    }
    
    // Combine all polygons together to produce the answer
    if (output != null) {
      for (Geometry finalPolygon : nonFinalPolygons)
        output.collect(finalPolygon);
    }
    resultSize += nonFinalPolygons.size();
      
    return resultSize;
  }
  
  /**
   * Computes the union of multiple groups of polygons. The algorithm runs in
   * the following steps.
   * <ol>
   *  <li>The polygons are flattened using {@link #flattenGeometries(Geometry[])}
   *   to extract the simplest form of them</li>
   *  <li>Polygons are grouped into groups of overlapping polygons using
   *  {@link #groupPolygons(Geometry[], Progressable)} so that we
   *   can compute the answer of each group separately</li>
   *  <li>The union of each group is computed using the {@link #unionGroup(Geometry[], Progressable, ResultCollector)}
   *   function</li>
   * </ol>
   * @param geoms
   * @param prog
   * @param output
   * @return
   * @throws IOException
   */
  public static int multiUnion(Geometry[] geoms, final Progressable prog,
      ResultCollector<Geometry> output) throws IOException {
    final Geometry[] basicShapes = flattenGeometries(geoms);
    prog.progress();
    
    final Geometry[][] groups = groupPolygons(basicShapes, prog);
    prog.progress();
    
    int resultSize = 0;
    for (Geometry[] group : groups) {
      resultSize += unionGroup(group, prog, output);
      prog.progress();
    }
    
    return resultSize;
  }
  
  public static long spatialJoinLocal(Path[] inFiles, Path outFile, OperationsParams params) throws IOException, InterruptedException {
      // Read the inputs and store them in memory
      List<Shape>[] datasets = new List[inFiles.length];
      final SpatialInputFormat3<Rectangle, Shape> inputFormat =
          new SpatialInputFormat3<Rectangle, Shape>();
      for (int i = 0; i < inFiles.length; i++) {
          datasets[i] = new ArrayList<Shape>();
          FileSystem inFs = inFiles[i].getFileSystem(params);
          Job job = Job.getInstance(params);
          SpatialInputFormat3.addInputPath(job, inFiles[i]);
          for (InputSplit split : inputFormat.getSplits(job)) {
              FileSplit fsplit = (FileSplit) split;
              RecordReader<Rectangle, Iterable<Shape>> reader =
                      inputFormat.createRecordReader(fsplit, null);
              if (reader instanceof SpatialRecordReader3) {
                  ((SpatialRecordReader3)reader).initialize(fsplit, params);
              } else if (reader instanceof RTreeRecordReader3) {
                  ((RTreeRecordReader3)reader).initialize(fsplit, params);
              } else if (reader instanceof HDFRecordReader) {
                  ((HDFRecordReader)reader).initialize(fsplit, params);
              } else {
                  throw new RuntimeException("Unknown record reader");
              }

              while (reader.nextKeyValue()) {
                  Iterable<Shape> shapes = reader.getCurrentValue();
                  for (Shape shape : shapes) {
                      datasets[i].add(shape.clone());
                  }
              }
              reader.close();
          }
      }

      // Apply the spatial join algorithm
      ResultCollector2<Shape, Shape> output = null;
      PrintStream out = null;
      if (outFile != null) {
          FileSystem outFS = outFile.getFileSystem(params);
          out = new PrintStream(outFS.create(outFile));
          final PrintStream outout = out;
          output = new ResultCollector2<Shape, Shape>() {
              @Override
              public void collect(Shape r, Shape s) {
                  outout.println(r.toText(new Text())+","+s.toText(new Text()));
              }
          };
      }
      long resultCount = SpatialJoin_planeSweep(datasets[0], datasets[1], output, null);
      
      if (out != null)
          out.close();
      
      return resultCount;
  }

}
