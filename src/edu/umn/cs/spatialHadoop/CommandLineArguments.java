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
package edu.umn.cs.spatialHadoop;

import java.awt.Color;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
import edu.umn.cs.spatialHadoop.core.OSMPolygon;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Polygon;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;


/**
 * Parses command line arguments.
 * @author Ahmed Eldawy
 *
 */
public class CommandLineArguments extends HashMap<String, Object> {
  private static final long serialVersionUID = 3470426865034003975L;

  /**All paths in the input parameter list*/
  public static final String ALL_PATHS = "all-paths";
  /**The output (last) path in the parameter list*/
  public static final String OUTPUT_PATH = "output-path";
  /**The first input path in the parameter list*/
  public static final String INPUT_PATH = "input-path";
  /**The input (all but last) paths in the parameter list*/
  public static final String INPUT_PATHS = "input-paths";
  /**Input rectangle. Either for range query or MBR for generation.*/
  public static final String INPUT_RECTANGLE = "rect";
  /**Input shape or input file format*/
  public static final String INPUT_SHAPE = "input-shape";

  private static final Log LOG = LogFactory.getLog(CommandLineArguments.class);
  
  private String[] args;

  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  public CommandLineArguments(String[] args) {
    this.args = args;
    
    Vector<Path> paths = new Vector<Path>();
    for (String arg : args) {
      String argl = arg.toLowerCase();
      if (arg.startsWith("-no-")) {
        this.put(argl.substring(4), false);
      } else if (argl.startsWith("-")) {
        this.put(argl.substring(1), true);
      } else if (argl.startsWith("rect:") || argl.startsWith("mbr:")) {
        Rectangle rect = new Rectangle();
        rect.fromText(new Text(argl.substring(argl.indexOf(':') + 1)));
        this.put(INPUT_RECTANGLE, rect);
      } else if (argl.startsWith("shape:")) {
        String shapeType = argl.substring(argl.indexOf(':') + 1);
        Shape inputShape = null;
        
        if (shapeType.startsWith("rect")) {
          inputShape = new Rectangle();
        } else if (shapeType.startsWith("point")) {
          inputShape = new Point();
        } else if (shapeType.startsWith("tiger")) {
          inputShape = new TigerShape();
        } else if (shapeType.startsWith("osm")) {
          inputShape = new OSMPolygon();
        } else if (shapeType.startsWith("poly")) {
          inputShape = new Polygon();
        } else if (shapeType.startsWith("ogc")) {
          inputShape = new OGCShape();
        } else if (shapeType.startsWith("nasa")) {
          inputShape = new NASAPoint();
        } else {
          // Use the shapeType as a class name and try to instantiate it dynamically
          try {
            Class<? extends Shape> shapeClass =
                Class.forName(arg.substring(arg.indexOf(':'))).asSubclass(Shape.class);
            inputShape = shapeClass.newInstance();
          } catch (ClassNotFoundException e) {
          } catch (InstantiationException e) {
          } catch (IllegalAccessException e) {
          }
        }
        if (inputShape == null)
          LOG.warn("unknown shape type: '"+arg.substring(arg.indexOf(':'))+"'");

        this.put(INPUT_SHAPE, inputShape);
      } else if (argl.startsWith("color:")) {
        String colorName = (String) argl.substring(argl.indexOf(':') + 1);
        Color color = Color.BLACK;
        colorName = colorName.toLowerCase();
        if (colorName.equals("red")) {
          color = Color.RED;
        } else if (colorName.equals("pink")){
          color = Color.PINK;
        } else if (colorName.equals("blue")){
          color = Color.BLUE;
        } else if (colorName.equals("cyan")){
          color = Color.CYAN;
        } else if (colorName.equals("green")) {
          color = Color.GREEN;
        } else if (colorName.equals("black")) {
          color = Color.BLACK;
        } else if (colorName.equals("gray")) {
          color = Color.GRAY;
        } else if (colorName.equals("orange")) {
          color = Color.ORANGE;
        }
        put("color", color);
      } else if (argl.contains(":") && !argl.contains(":/")) {
        String[] parts = arg.split(":", 2);
        this.put(parts[0].toLowerCase(), parts[1]);
      } else {
        paths.add(new Path(arg));
      }
    }

    Path[] allPaths = paths.toArray(new Path[paths.size()]);
    Path outputPath = paths.size() > 1? paths.remove(paths.size() - 1) : null;
    Path[] inputPaths = paths.toArray(new Path[paths.size()]);
    Path inputPath = inputPaths[0];
    
    this.put(ALL_PATHS, allPaths);
    this.put(INPUT_PATH, inputPath);
    this.put(INPUT_PATHS, inputPaths);
    if (outputPath != null)
      this.put(OUTPUT_PATH, outputPath);
  }
  
  public Map<String, Object> getParams() {
    return this;
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
    return (Path[]) get(ALL_PATHS);
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
  
  public boolean is(String flag) {
    return is(flag, false);
  }
  
  public boolean is(String flag, boolean defaultValue) {
    if (containsKey(flag))
      return (Boolean) get(flag);
    return defaultValue;
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
   * Return the command with the given key. If the key does not exist,
   * the default value is returned.
   * @param key
   * @param defaultValue
   * @return
   */
  public String get(String key, String defaultValue) {
    key = key.toLowerCase() +":";
    for (String arg : args) {
      if (arg.toLowerCase().startsWith(key)) {
        return arg.substring(arg.indexOf(':')+1);
      }
    }
    return defaultValue;
  }
  
  public int getInt(String key, int defaultValue) {
    String valstr = (String) get(key);
    return valstr == null ? defaultValue : Integer.parseInt(valstr);
  }
  
  public CellInfo[] getCells() {
    String cell_of = (String) get("cells-of");
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
  

  public long getSeed() {
    String seed = (String) get("seed");
    return seed == null? System.currentTimeMillis() : Long.parseLong(seed);
  }

  public int getRectSize() {
    String rectSize = (String) get("rectsize");
    return rectSize == null? 0 : Integer.parseInt(rectSize);
  }

  public double getClosenessFactor() {
    String factor = (String) get("closeness");
    return factor == null? -1.0 : Double.parseDouble(factor);
  }
  
  public long getOffset() {
    String offset = (String) get("offset");
    return offset == null? -1 : Long.parseLong(offset);
  }

  public boolean isBorders() {
    return is("borders");
  }

  public Shape getOutputShape() {
    String shapeTypeStr = (String) get("outshape");
    if (shapeTypeStr == null)
      shapeTypeStr = (String) get("outputshape");
    if (shapeTypeStr == null)
      shapeTypeStr = (String) get("output_shape");
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

  /**
   * Makes standard checks for input and output files. It is assumed that all
   * files are input files while the last one is the output file. First,
   * it checks that there is at least one input file. Then, it checks that every
   * input file exists. After that, it checks for output file, if it exists and
   * the overwrite flag is not present, it fails.
   * @return <code>true</code> if all checks pass. <code>false</code> otherwise.
   * @throws IOException 
   */
  public boolean checkInputOutput(Configuration conf) throws IOException {
    Path[] inputPaths = (Path[]) get(INPUT_PATHS);
    for (Path path : inputPaths) {
      if (isWildcard(path))
        continue;
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        LOG.error("Input file '"+path+"' does not exist");
        return false;
      }
    }
    Path outputPath = (Path) get(OUTPUT_PATH);
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(conf);
      if (fs.exists(outputPath)) {
        if (this.is("overwrite")) {
          fs.delete(outputPath, true);
        } else {
          LOG.error("Output file '"+outputPath+"' exists and overwrite flag is not set");
          return false;
        }
      }
    }
    return true;
  }

  public static boolean isWildcard(Path path) {
    return path.toString().indexOf('*') != -1 ||
        path.toString().indexOf('?') != -1;
  }

  public Path getOutputPath() {
    return (Path) get(OUTPUT_PATH);
  }

  public Path getInputPath() {
    return getPath();
  }

  public Color getColor(String key, Color defaultValue) {
    if (containsKey(key))
      return (Color) get(key);
    return defaultValue;
  }
}
