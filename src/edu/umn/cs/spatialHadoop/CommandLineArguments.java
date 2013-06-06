package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.OGCShape;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Polygon;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.operations.Sampler;

public class CommandLineArguments {
  private static final Log LOG = LogFactory.getLog(CommandLineArguments.class);
  
  private String[] args;

  public CommandLineArguments(String[] args) {
    this.args = args;
  }
  
  public Rectangle getRectangle() {
    Rectangle rect = null;
    for (String arg : args) {
      if (arg.startsWith("rect:") || arg.startsWith("rectangle:") || arg.startsWith("mbr:")) {
        rect = new Rectangle();
        rect.fromText(new Text(arg.substring(arg.indexOf(':')+1)));
      }
    }
    return rect;
  }
  
  public Rectangle[] getRectangles() {
    Vector<Rectangle> rectangles = new Vector<Rectangle>();
    for (String arg : args) {
      if (arg.startsWith("rect:") || arg.startsWith("rectangle:") || arg.startsWith("mbr:")) {
        Rectangle rect = new Rectangle();
        rect.fromText(new Text(arg.substring(arg.indexOf(':')+1)));
        rectangles.add(rect);
      }
    }
    return rectangles.toArray(new Rectangle[rectangles.size()]);
  }
  
  public Path[] getPaths() {
    Vector<Path> inputPaths = new Vector<Path>();
    for (String arg : args) {
      if (arg.startsWith("-") && arg.length() > 1) {
        // Skip
      } else if (arg.indexOf(':') != -1 && arg.indexOf(":/") == -1) {
        // Skip
      } else {
        inputPaths.add(new Path(arg));
      }
    }
    return inputPaths.toArray(new Path[inputPaths.size()]);
  }
  
  public Path getPath() {
    Path[] paths = getPaths();
    return paths.length > 0? paths[0] : null;
  }
  
  public GridInfo getGridInfo() {
    GridInfo grid = null;
    for (String arg : args) {
      if (arg.startsWith("grid:")) {
        grid = new GridInfo();
        grid.fromText(new Text(arg.substring(arg.indexOf(':')+1)));
      }
    }
    return grid;
  }

  public Point getPoint() {
    Point point = null;
    for (String arg : args) {
      if (arg.startsWith("point:")) {
        point = new Point();
        point.fromText(new Text(arg.substring(arg.indexOf(':')+1)));
      }
    }
    return point;
  }

  public Point[] getPoints() {
    Vector<Point> points = new Vector<Point>();
    for (String arg : args) {
      if (arg.startsWith("point:")) {
        Point point = new Point();
        point.fromText(new Text(arg.substring(arg.indexOf(':')+1)));
        points.add(point);
      }
    }
    return points.toArray(new Point[points.size()]);
  }

  public boolean isOverwrite() {
    return is("overwrite");
  }
  
  public long getSize() {
    for (String arg : args) {
      if (arg.startsWith("size:")) {
        String size_str = arg.split(":")[1];
        if (size_str.indexOf('.') == -1)
          return Long.parseLong(size_str);
        String[] size_parts = size_str.split("\\.", 2);
        long size = Long.parseLong(size_parts[0]);
        size_parts[1] = size_parts[1].toLowerCase();
        if (size_parts[1].startsWith("k"))
          size *= 1024;
        else if (size_parts[1].startsWith("m"))
          size *= 1024 * 1024;
        else if (size_parts[1].startsWith("g"))
          size *= 1024 * 1024 * 1024;
        else if (size_parts[1].startsWith("t"))
          size *= 1024 * 1024 * 1024 * 1024;
        return size;
      }
    }
    return 0;
  }

  public boolean isRandom() {
    return is("random");
  }
  
  public boolean isLocal() {
    return is("local");
  }
  
  /**
   * Whether the user asked for an explicit repartition step or not
   * @return
   */
  public String getRepartition() {
    return get("repartition");
  }
  
  public boolean is(String flag) {
    String expected_arg = "-"+flag;
    for (String arg : args) {
      if (arg.equals(expected_arg))
        return true;
    }
    return false;
  }

  public int getCount() {
    for (String arg : args) {
      if (arg.startsWith("count:")) {
        return Integer.parseInt(arg.substring(arg.indexOf(':')+1));
      }
    }
    return 1;
  }
  
  public int getK() {
    for (String arg : args) {
      if (arg.startsWith("k:")) {
        return Integer.parseInt(arg.substring(arg.indexOf(':')+1));
      }
    }
    return 0;
  }

  public float getSelectionRatio() {
    for (String arg : args) {
      if (arg.startsWith("ratio:")) {
        return Float.parseFloat(arg.substring(arg.indexOf(':')+1));
      }
    }
    return -1.0f;
  }

  public int getConcurrency() {
    for (String arg : args) {
      if (arg.startsWith("concurrency:")) {
        return Integer.parseInt(arg.substring(arg.indexOf(':')+1));
      }
    }
    return Integer.MAX_VALUE;
  }

  public long getBlockSize() {
    for (String arg : args) {
      if (arg.startsWith("blocksize:") || arg.startsWith("block_size:")) {
        String size_str = arg.split(":")[1];
        if (size_str.indexOf('.') == -1)
          return Long.parseLong(size_str);
        String[] size_parts = size_str.split("\\.", 2);
        long size = Long.parseLong(size_parts[0]);
        size_parts[1] = size_parts[1].toLowerCase();
        if (size_parts[1].startsWith("k"))
          size *= 1024;
        else if (size_parts[1].startsWith("m"))
          size *= 1024 * 1024;
        else if (size_parts[1].startsWith("g"))
          size *= 1024 * 1024 * 1024;
        else if (size_parts[1].startsWith("t"))
          size *= 1024 * 1024 * 1024 * 1024;
        return size;
      }
    }
    return 0;
  }
  
  /**
   * Finds any parameters that has with the given key name
   * @param key
   * @return
   */
  protected String get(String key) {
    key = key +":";
    for (String arg : args) {
      if (arg.startsWith(key)) {
        return arg.substring(arg.indexOf(':')+1);
      }
    }
    return null;
  }
  
  /**
   * 
   * @param autodetect - Automatically detect shape type from input file
   *   if shape is not explicitly set by user
   * @return
   */
  public Shape getShape(boolean autodetect) {
    String shapeTypeStr = get("shape");
    final Text shapeType = new Text();
    if (shapeTypeStr != null)
      shapeType.set(shapeTypeStr.toLowerCase().getBytes());
    
    if (autodetect && shapeType.getLength() == 0 && getPath() != null) {
      // Shape type not found in parameters. Try to infer from a line in input
      // file
      Path in_file = getPath();
      try {
        Sampler.sampleLocal(in_file.getFileSystem(new Configuration()), in_file, 1, 0, new ResultCollector<Text2>() {
          @Override
          public void collect(Text2 value) {
            String val = value.toString();
            String[] parts = val.split(",");
            if (parts.length == 2) {
              shapeType.set("point".getBytes());
            } else if (parts.length == 4) {
              shapeType.set("rect".getBytes());
            } else if (parts.length > 4) {
              shapeType.set("tiger".getBytes());
            }
          }
        }, new Text2(), new Text2());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    Shape stockShape = null;
    if (shapeType.toString().startsWith("rect")) {
      stockShape = new Rectangle();
    } else if (shapeType.toString().startsWith("point")) {
      stockShape = new Point();
    } else if (shapeType.toString().startsWith("tiger")) {
      stockShape = new TigerShape();
    } else if (shapeType.toString().startsWith("poly")) {
      stockShape = new Polygon();
    } else if (shapeType.toString().startsWith("ogc")) {
      stockShape = new OGCShape();
    } else if (shapeTypeStr != null) {
      // Use the shapeType as a class name and try to instantiate it dynamically
      try {
        Class<? extends Shape> shapeClass =
            Class.forName(shapeTypeStr).asSubclass(Shape.class);
        stockShape = shapeClass.newInstance();
      } catch (ClassNotFoundException e) {
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      }
    }
    if (stockShape == null)
      LOG.warn("unknown shape type: "+shapeTypeStr);
    
    return stockShape;
  }
  
  public CellInfo[] getCells() {
    String cell_of = get("cells-of");
    if (cell_of == null)
      return null;
    Path path = new Path(cell_of);
    FileSystem fs;
    try {
      fs = path.getFileSystem(new Configuration());
      return SpatialSite.cellsOf(fs, path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * Return the type of the global index to be used. This could be either:
   * "grid", "rtree" or <code>null</code>.
   * These are the values currently supported by the system but the user can
   * provide any text value.
   * @return
   */
  public String getGIndex() {
    String gindex = get("gindex");
    if (gindex == null)
      gindex = get("global");
    return gindex;
  }
  
  /**
   * Return the type of the local index to be used. This could be either:
   * "grid", "rtree" or <code>null</code>.
   * These are the values currently supported by the system but the user can
   * provide any text value.
   * @return
   */
  public String getLIndex() {
    String lindex = get("lindex");
    if (lindex == null)
      lindex = get("local");
    return lindex; 
  }
  

  public long getSeed() {
    String seed = get("seed");
    return seed == null? System.currentTimeMillis() : Long.parseLong(seed);
  }

  public int getRectSize() {
    String rectSize = get("rectsize");
    return rectSize == null? 0 : Integer.parseInt(rectSize);
  }

  public double getClosenessFactor() {
    String factor = get("closeness");
    return factor == null? -1.0 : Double.parseDouble(factor);
  }
  
  public long getOffset() {
    String offset = get("offset");
    return offset == null? -1 : Long.parseLong(offset);
  }

  public boolean isBorders() {
    return is("borders");
  }

  public int getWidth(int default_width) {
    String width = get("width");
    return width == null ? default_width : Integer.parseInt(width);
  }

  public int getHeight(int default_height) {
    String height = get("height");
    return height == null ? default_height : Integer.parseInt(height);
  }

  public Shape getOutputShape() {
    String shapeTypeStr = get("outshape");
    if (shapeTypeStr == null)
      shapeTypeStr = get("outputshape");
    if (shapeTypeStr == null)
      shapeTypeStr = get("output_shape");
    final Text shapeType = new Text();
    if (shapeTypeStr != null)
      shapeType.set(shapeTypeStr.toLowerCase().getBytes());
    
    Shape stockShape = null;
    if (shapeType.toString().startsWith("rect")) {
      stockShape = new Rectangle();
    } else if (shapeType.toString().startsWith("point")) {
      stockShape = new Point();
    } else if (shapeType.toString().startsWith("tiger")) {
      stockShape = new TigerShape();
    } else if (shapeType.toString().startsWith("poly")) {
      stockShape = new Polygon();
    } else if (shapeType.toString().startsWith("ogc")) {
      stockShape = new OGCShape();
    } else if (shapeTypeStr != null) {
      // Use the shapeType as a class name and try to instantiate it dynamically
      try {
        Class<? extends Shape> shapeClass =
            Class.forName(shapeTypeStr).asSubclass(Shape.class);
        stockShape = shapeClass.newInstance();
      } catch (ClassNotFoundException e) {
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      }
    }
    if (stockShape == null)
      LOG.warn("unknown shape type: "+shapeTypeStr);
    
    return stockShape;
  }
}
