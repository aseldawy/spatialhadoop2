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
import java.lang.reflect.Array;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.OGCShape;
import edu.umn.cs.spatialHadoop.core.OSMPolygon;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Polygon;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;


/**
 * Parses command line arguments.
 * @author Ahmed Eldawy
 *
 */
public class CommandLineArguments extends Configuration {
  private static final Log LOG = LogFactory.getLog(CommandLineArguments.class);

  /**All detected input paths*/
  private Path[] allPaths;

  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  public CommandLineArguments(String... args) {
    Vector<Path> paths = new Vector<Path>();
    for (String arg : args) {
      String argl = arg.toLowerCase();
      if (arg.startsWith("-no-")) {
        this.setBoolean(argl.substring(4), false);
      } else if (argl.startsWith("-")) {
        this.setBoolean(argl.substring(1), true);
      } else if (argl.contains(":") && !argl.contains(":/")) {
        String[] parts = arg.split(":", 2);
        String key = parts[0].toLowerCase();
        String value = parts[1];
        String previousValue = this.get(key);
        if (previousValue == null)
          this.set(key, value);
        else
          this.set(key, previousValue+"\n"+value);
      } else {
        paths.add(new Path(arg));
      }
    }
    this.allPaths = paths.toArray(new Path[paths.size()]);
  }
  
  public Path[] getPaths() {
    return allPaths;
  }
  
  public Path getPath() {
    return allPaths.length > 0 ? allPaths[0] : null;
  }
  
  public Path getOutputPath() {
    return allPaths.length > 1 ? allPaths[allPaths.length - 1] : null;
  }

  public Path getInputPath() {
    return getPath();
  }

  public Path[] getInputPaths() {
    if (allPaths.length < 2) {
      return allPaths;
    }
    Path[] inputPaths = new Path[allPaths.length - 1];
    System.arraycopy(allPaths, 0, inputPaths, 0, inputPaths.length);
    return inputPaths;
  }

/*  public long getSize() {
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
*/
  public boolean is(String flag, boolean defaultValue) {
    return getBoolean(flag, defaultValue);
  }

  public boolean is(String flag) {
    return is(flag, false);
  }

/*  public long getBlockSize() {
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
*/  
  
/*  public CellInfo[] getCells() {
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
 */ 

  /**
   * Makes standard checks for input and output files. It is assumed that all
   * files are input files while the last one is the output file. First,
   * it checks that there is at least one input file. Then, it checks that every
   * input file exists. After that, it checks for output file, if it exists and
   * the overwrite flag is not present, it fails.
   * @return <code>true</code> if all checks pass. <code>false</code> otherwise.
   * @throws IOException 
   */
  public boolean checkInputOutput() throws IOException {
    Path[] inputPaths = getInputPaths();
    if (inputPaths.length == 0) {
      LOG.error("No input files");
      return false;
    }
    for (Path path : inputPaths) {
      if (isWildcard(path))
        continue;
      FileSystem fs = path.getFileSystem(this);
      if (!fs.exists(path)) {
        LOG.error("Input file '"+path+"' does not exist");
        return false;
      }
    }
    Path outputPath = getOutputPath();
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(this);
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

  public Color getColor(String key, Color defaultValue) {
    String colorName = get(key);
    if (colorName == null)
      return defaultValue;
    
    colorName = colorName.toLowerCase();
    Color color = defaultValue;
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
    
    return color;
  }
  
  public Shape getShape(String key, Shape defaultValue) {
    String shapeType = get(key);
    if (shapeType == null)
      return defaultValue;
    
    shapeType = shapeType.toLowerCase();
    Shape shape = null;
    
    if (shapeType.startsWith("rect")) {
      shape = new Rectangle();
    } else if (shapeType.startsWith("point")) {
      shape = new Point();
    } else if (shapeType.startsWith("tiger")) {
      shape = new TigerShape();
    } else if (shapeType.startsWith("osm")) {
      shape = new OSMPolygon();
    } else if (shapeType.startsWith("poly")) {
      shape = new Polygon();
    } else if (shapeType.startsWith("ogc")) {
      shape = new OGCShape();
    } else if (shapeType.startsWith("nasa")) {
      shape = new NASAPoint();
    } else {
      // Use the shapeType as a class name and try to instantiate it dynamically
      try {
        Class<? extends Shape> shapeClass =
            getClassByName(get(key)).asSubclass(Shape.class);
        shape = shapeClass.newInstance();
      } catch (ClassNotFoundException e) {
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      }
      if (shape == null) {
        // Couldn't detect shape from short name or full class name
        // May be it's an actual value that we can parse
        if (shapeType.split(",").length == 2) {
          // A point
          shape = new Point();
          shape.fromText(new Text((String)get(key)));
        } else if (shapeType.split(",").length == 4) {
          // A rectangle
          shape = new Rectangle();
          shape.fromText(new Text((String)get(key)));
        }
        // TODO parse from WKT
      }
    }
    if (shape == null)
      LOG.warn("unknown shape type: '"+get(key)+"'");
    return shape;
  }
  
  public Shape getShape(String key) {
    return getShape(key, null);
  }

  public <S extends Shape> S[] getShapes(String key, S stock) {
    String[] values = getArray(key);
    S[] shapes = (S[]) Array.newInstance(stock.getClass(), values.length);
    for (int i = 0; i < values.length; i++) {
      shapes[i] = (S) stock.clone();
      shapes[i].fromText(new Text(values[i]));
    }
    return shapes;
  }
  
  private String[] getArray(String key) {
    String val = get(key);
    return val == null ? null : val.split("\n");
  }

  public long getSize(String key) {
    String size_str = get(key);
    if (size_str == null)
      return 0;
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
