package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * @author eldawy
 *
 */
public abstract class SpatialInputFormat<K, V> extends FileInputFormat<K, V> {
  
  private CompressionCodecFactory compressionCodecs = null;
  
  /**
   * We need to use this way of constructing readers to be able to pass it to
   * CmobineFileRecordReader
   **/
  @SuppressWarnings("rawtypes")
  static final Class [] constructorSignature = new Class [] 
      { Configuration.class, 
       FileSplit.class};
  
  @SuppressWarnings("rawtypes")
  protected Class<? extends RecordReader> rrClass;
  
  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    if (compressionCodecs == null)
      compressionCodecs = new CompressionCodecFactory(job);
    if (split instanceof CombineFileSplit) {
      return new CombineFileRecordReader<K, V>(job, (CombineFileSplit)split,
          reporter, (Class<RecordReader<K, V>>) rrClass);
    } else if (split instanceof FileSplit) {
      try {
        @SuppressWarnings("rawtypes")
        Constructor<? extends RecordReader> rrConstructor;
        rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
        rrConstructor.setAccessible(true);
        return rrConstructor.newInstance(new Object [] {job, (FileSplit)split});
      } catch (SecurityException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      }
      throw new RuntimeException("Cannot generate a record reader");
    } else {
      throw new RuntimeException("Cannot handle splits of type "+split.getClass());
    }
  }
  
  public static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName(); 
      return !name.startsWith("_") && !name.startsWith("."); 
    }
  };
  
  protected void listStatus(final FileSystem fs, final Path dir,
      final List<FileStatus> result, BlockFilter filter) throws IOException {
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, dir);
    FileStatus[] listStatus = fs.listStatus(dir, hiddenFileFilter);
    if (gindex == null) {
      // Add all files under this directory
      for (FileStatus status : listStatus) {
        if (status.isDir()) {
          listStatus(fs, status.getPath(), result, filter);
        } else {
          result.add(status);
        }
      }
    } else {
      // Use the global index to limit files
      filter.selectCells(gindex, new ResultCollector<Partition>() {
        @Override
        public void collect(Partition partition) {
          try {
            LOG.info("User filter function matched with "+partition);
            Path cell_path = new Path(dir, partition.filename);
            if (!fs.exists(cell_path))
              LOG.warn("Matched file not found: "+cell_path);
            result.add(fs.getFileStatus(cell_path));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }
  
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    try {
      if (compressionCodecs == null)
        compressionCodecs = new CompressionCodecFactory(job);
      Class<? extends BlockFilter> blockFilterClass =
          job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass == null) {
        LOG.info("No block filter specified");
        // No block filter specified by user
        return super.listStatus(job);
      }
      // Get all blocks the user wants to process
      BlockFilter blockFilter;
      blockFilter = blockFilterClass.newInstance();
      blockFilter.configure(job);
      
      // Filter files based on user specified filter function
      List<FileStatus> result = new ArrayList<FileStatus>();
      Path[] dirs = getInputPaths(job);
      
      for (Path dir : dirs) {
        FileSystem fs = dir.getFileSystem(job);
        listStatus(fs, dir, result, blockFilter);
      }
      
      LOG.info("Spatial filter function matched with "+result.size()+" cells");
      
      return result.toArray(new FileStatus[result.size()]);
    } catch (InstantiationException e) {
      LOG.warn(e);
      return super.listStatus(job);
    } catch (IllegalAccessException e) {
      LOG.warn(e);
      return super.listStatus(job);
    }
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec != null && !(codec instanceof SplittableCompressionCodec))
      return false;

    try {
      return !SpatialSite.isRTree(fs, file);
    } catch (IOException e) {
      return super.isSplitable(fs, file);
    }
  }
}
