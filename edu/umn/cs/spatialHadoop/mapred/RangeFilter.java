package edu.umn.cs.spatialHadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;


public class RangeFilter extends DefaultBlockFilter {
  
  private static final Log LOG = LogFactory.getLog(RangeFilter.class);
  
  /**Name of the config line that stores the class name of the query shape*/
  private static final String QUERY_SHAPE_CLASS = "RangeFilter.QueryShapeClass";

  /**Name of the config line that stores the query shape*/
  private static final String QUERY_SHAPE = "RangeFilter.QueryShape";

  /**A shape that is used to filter input*/
  private Shape queryRange;
  
  /**
   * Sets the query range in the given job.
   * @param job
   * @param shape
   */
  public static void setQueryRange(JobConf job, Shape shape) {
    job.setClass(QUERY_SHAPE_CLASS, shape.getClass(), Shape.class);
    job.set(QUERY_SHAPE, shape.toText(new Text()).toString());
  }
  
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    try {
      String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
      Class<? extends Shape> queryShapeClass =
          Class.forName(queryShapeClassName).asSubclass(Shape.class);
      queryRange = queryShapeClass.newInstance();
      queryRange.fromText(new Text(job.get(QUERY_SHAPE)));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void selectCells(GlobalIndex<Partition> gIndex,
      ResultCollector<Partition> output) {
    int numPartitions = gIndex.rangeQuery(queryRange, output);
    LOG.info("Selected "+numPartitions+" partitions in the range "+queryRange);
  }
}