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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


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
      OutputCollector<S, S> output, Reporter reporter) throws IOException {
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
      boolean refine, final OutputCollector<S, S> output, Reporter reporter) throws IOException {
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
}
