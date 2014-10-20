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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.CSVOGC;
import edu.umn.cs.spatialHadoop.core.OGCESRIShape;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Polygon;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint.GradientType;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;


/**
 * A class that encapsulates all parameters sent for an operations implemented
 * in SpatialHadoop. Internally, it stores everything in {@link Configuration}
 * which makes it easier to pass around to mappers and reducers.
 * It can be initialized from a configuration and/or a list of command line
 * arguments.
 * @author Ahmed Eldawy
 *
 */
public class OperationsParams extends Configuration {
  private static final Log LOG = LogFactory.getLog(OperationsParams.class);
  
  /**Separator between shape type and value*/
  public static final String ShapeValueSeparator = "//";

  /**All detected input paths*/
  private Path[] allPaths;

  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  public OperationsParams() {
  }

  public OperationsParams(GenericOptionsParser parser) {
    this(parser, true);
  }

  public OperationsParams(GenericOptionsParser parser, boolean autodetectShape) {
    super(parser.getConfiguration());
    initialize(parser.getRemainingArgs());
    if (autodetectShape) {
      TextSerializable shape = getShape("shape");
      if (shape != null) {
        // In case this class is in a third part jar file, add it to path
        SpatialSite.addClassToPath(this, shape.getClass());
      }
    }
  }

  /**
   * Initialize the command line arguments from an existing configuration and
   * a list of additional arguments.
   * @param params
   * @param additionalArgs
   */
  public OperationsParams(Configuration params, String ... additionalArgs) {
    super(params);
    initialize(additionalArgs);
  }

  public void initialize(String... args) {
    // TODO if the argument shape is set to a class in a third party jar file
    // add that jar file to the archives
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

  public boolean is(String flag, boolean defaultValue) {
    return getBoolean(flag, defaultValue);
  }

  public boolean is(String flag) {
    return is(flag, false);
  }

  
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
   * Checks that there is at least one input path and that all input paths
   * exist. It treats all passed paths as input. This is useful for input-only
   * operations which do not take any output path (e.g., MBR and Aggregate).
   * @return
   * @throws IOException
   */
  public boolean checkInput() throws IOException {
    Path[] inputPaths = getPaths();
    if (inputPaths.length == 0) {
      LOG.error("No input files");
      return false;
    }
    
    for (Path path : inputPaths) {
      // Skip existence checks for wild card input
      if (isWildcard(path))
        continue;
      FileSystem fs = path.getFileSystem(this);
      if (!fs.exists(path)) {
        LOG.error("Input file '"+path+"' does not exist");
        return false;
      }
    }
    
    return true;
  }
  
  public boolean checkInputOutput() throws IOException {
    return checkInputOutput(false);
  }

  /**
   * Makes standard checks for input and output files. It is assumes that all
   * files are input files while the last one is the output file. First,
   * it checks that there is at least one input file. Then, it checks that every
   * input file exists. After that, it checks for output file, if it exists and
   * the overwrite flag is not present, it fails.
   * @return <code>true</code> if all checks pass. <code>false</code> otherwise.
   * @throws IOException 
   */
  public boolean checkInputOutput(boolean outputRequired) throws IOException {
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
    if (outputPath == null && outputRequired) {
      LOG.error("Output path is missing");
      return false;
    }
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
  
  public boolean checkOutput() throws IOException {
    Path[] inputPaths = getInputPaths();
    Path outputPath = inputPaths[inputPaths.length - 1];
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
    return getColor(this, key, defaultValue);
  }
  
  public static Color getColor(Configuration conf, String key, Color defaultValue) {
    String colorName = conf.get(key);
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
    } else if (colorName.equals("white")) {
      color = Color.WHITE;
    } else if (colorName.equals("gray")) {
      color = Color.GRAY;
    } else if (colorName.equals("yellow")) {
      color = Color.YELLOW;
    } else if (colorName.equals("orange")) {
      color = Color.ORANGE;
    } else if (colorName.equals("none")) {
      color = new Color(0, true);
    } else {
      LOG.warn("Does not understand the color '"+conf.get(key)+"'");
    }
    
    return color;
  }
  
  public Shape getShape(String key, Shape defaultValue) {
    if (defaultValue == null)
      autoDetectShape();
    return getShape(this, key, defaultValue);
  }
  
  public Shape getShape(String key) {
    return getShape(key, null);
  }
  
  public static Shape getShape(Configuration job, String key) {
    return getShape(job, key, null);
  }

  public static Shape getShape(Configuration conf, String key, Shape defaultValue) {
    TextSerializable t = getTextSerializable(conf, key, defaultValue);
    return t instanceof Shape ? (Shape) t : null;
  }
  
  public static TextSerializable getTextSerializable(Configuration conf, String key, TextSerializable defaultValue) {
    String shapeType = conf.get(key);
    if (shapeType == null)
      return defaultValue;
    
    int separatorIndex = shapeType.indexOf(ShapeValueSeparator);
    Text shapeValue = null;
    if (separatorIndex != -1) {
      shapeValue = new Text(shapeType.substring(separatorIndex + ShapeValueSeparator.length()));
      shapeType = shapeType.substring(0, separatorIndex);
    }
    
    String shapeTypeI = shapeType.toLowerCase();
    TextSerializable shape = null;
    
    if (shapeTypeI.startsWith("rect")) {
      shape = new Rectangle();
    } else if (shapeTypeI.startsWith("point")) {
      shape = new Point();
    } else if (shapeTypeI.startsWith("tiger")) {
      shape = new TigerShape();
    } else if (shapeTypeI.startsWith("osm")) {
      shape = new OSMPolygon();
    } else if (shapeTypeI.startsWith("poly")) {
      shape = new Polygon();
    } else if (shapeTypeI.startsWith("ogc")) {
      shape = new OGCESRIShape();
    } else if (shapeTypeI.startsWith("wkt")) {
      shape = new OGCJTSShape();
    } else if (shapeTypeI.startsWith("nasa")) {
      shape = new NASAPoint();
    } else if (shapeTypeI.startsWith("text")) {
      shape = new Text2();
    } else {
      // Use the shapeType as a class name and try to instantiate it dynamically
      try {
        Class<? extends TextSerializable> shapeClass =
            conf.getClassByName(shapeType).asSubclass(TextSerializable.class);
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
          shape.fromText(new Text((String)conf.get(key)));
        } else if (shapeType.split(",").length == 4) {
          // A rectangle
          shape = new Rectangle();
          shape.fromText(new Text((String)conf.get(key)));
        }
        // TODO parse from WKT
      }
    }
    if (shape == null)
      LOG.warn("unknown shape type: '"+conf.get(key)+"'");
    else if (shapeValue != null)
      shape.fromText(shapeValue);
    // Special case for CSVOGC shape, specify the column if possible
    if (shape instanceof CSVOGC) {
      CSVOGC csvShape = (CSVOGC) shape;
      String strColumnIndex = conf.get("column");
      if (strColumnIndex != null)
        csvShape.setColumn(Integer.parseInt(strColumnIndex));
      String strSeparator = conf.get("separator");
      if (strSeparator != null)
        csvShape.setSeparator(strSeparator.charAt(0));
    }
    return shape;
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
    return getSize(this, key);
  }
  
  /**
   * Sets the specified configuration parameter to the current value of the shape.
   * Both class name and shape values are encoded in one string and set as the
   * value of the configuration parameter. The shape can be retrieved later
   * using {@link SpatialSite#getShape(Configuration, String)}.
   * @param conf
   * @param param
   * @param shape
   */
  public static void setShape(Configuration conf, String param, Shape shape) {
    String str = shape.getClass().getName() + ShapeValueSeparator;
    str += shape.toText(new Text()).toString();
    conf.set(param, str);
  }

  public static long getSize(Configuration conf, String key) {
    String size_str = conf.get(key);
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
  
  /**Data type for the direction of skyline to compute*/
  public enum Direction {MaxMax, MaxMin, MinMax, MinMin};

  public Direction getDirection(String key, Direction defaultDirection) { 
    return getDirection(this, key, defaultDirection);
  }
  
  public static Direction getDirection(Configuration conf, String key, Direction defaultDirection) {
    String strdir = conf.get("dir");
    if (strdir == null)
      return defaultDirection;
    Direction dir;
    if (strdir.equalsIgnoreCase("maxmax") || strdir.equalsIgnoreCase("max-max")) {
      dir = Direction.MaxMax;
    } else if (strdir.equalsIgnoreCase("maxmin") || strdir.equalsIgnoreCase("max-min")) {
      dir = Direction.MaxMin;
    } else if (strdir.equalsIgnoreCase("minmax") || strdir.equalsIgnoreCase("min-max")) {
      dir = Direction.MinMax;
    } else if (strdir.equalsIgnoreCase("minmin") || strdir.equalsIgnoreCase("min-min")) {
      dir = Direction.MinMin;
    } else {
      System.err.println("Invalid direction: "+strdir);
      System.err.println("Valid directions are: max-max, max-min, min-max, and min-min");
      return null;
    }
    return dir;
  }

  public NASAPoint.GradientType getGradientType(String key, GradientType defaultValue) {
    return getGradientType(this, key, defaultValue);
  }
  
  public static NASAPoint.GradientType getGradientType(Configuration conf, String key, GradientType defaultValue) {
    String strGradientType = conf.get(key);
    if (strGradientType == null) {
      return defaultValue;
    } else if (strGradientType.equalsIgnoreCase("hue")) {
      return GradientType.GT_HUE;
    } else if (strGradientType.equalsIgnoreCase("color")) {
      return GradientType.GT_COLOR;
    } else {
      LOG.warn("Unknown gradient type '"+strGradientType+"'. Possible values are 'hue', 'color'");
      return defaultValue;
    }
  }

  /**
   * Auto detect shape type based on input format
   * 
   * @return <code>true</code> if a shape is already present in the
   *         configuration or it is auto detected from input files.
   *         <code>false</code> if it was not set and could not be detected.
   */
  public boolean autoDetectShape() {
    String autoDetectedShape = null;
    final Vector<String> sampleLines = new Vector<String>();
    if (this.get("shape") != null)
      return true; // A shape is already configured
    if (this.getInputPaths().length == 0)
      return false; // No shape is configured and no input to detected
    try {
      // Read a random sample from the input
      // We read a sample instead of one line to make sure
      // the auto detected shape is consistent in many lines
      final int sampleCount = 10;
      OperationsParams sampleParams = new OperationsParams(this);
      sampleParams.setInt("count", sampleCount);
      sampleParams.setClass("shape", Text2.class, TextSerializable.class);
      sampleParams.setClass("outshape", Text2.class, TextSerializable.class);
      sampleParams.setBoolean("local", true);
      Sampler.sample(this.getInputPaths(), new ResultCollector<Text2>() {
        @Override
        public void collect(Text2 line) {
          sampleLines.add(line.toString());
        }
      }, sampleParams);
      
      // Collect some stats about the sample to help detecting shape type
      final String Separators[] = {",", "\t"};
      int[] numOfSplits = {0, 0}; // Number of splits with each separator
      boolean allNumbersInAllSample = true; // Whether or not all values are numbers
      int ogcIndex = -1; // The index of the column with OGC data

      for (String sampleLine : sampleLines) {
        // This flag is raised if all splits are numbers with any separator
        boolean allNumbersWithAnySeparator = false;
        // Try to parse with commas and tabs
        for (int iSeparator = 0; iSeparator < Separators.length; iSeparator++) {
          String separator = Separators[iSeparator];
          String[] parts = sampleLine.split(separator);
          
          if (numOfSplits[iSeparator] == 0)
            numOfSplits[iSeparator] = parts.length;
          else if (numOfSplits[iSeparator] != parts.length)
            numOfSplits[iSeparator] = -1;
          boolean allSplitsNumbersWithCurrentSeparator = true;
          for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            try {
              Double.parseDouble(part);
            } catch (NumberFormatException e) {
              allSplitsNumbersWithCurrentSeparator = false;
            }
            try {
              TextSerializerHelper.consumeGeometryJTS(new Text(part), '\0');
              // Reaching this point means the geometry was parsed successfully
              if (ogcIndex == -1)
                ogcIndex = i;
              else if (ogcIndex != i)
                ogcIndex = -2;
            } catch (Exception e) {
              // Couldn't parse OGC for this column
            }
          }
          if (allSplitsNumbersWithCurrentSeparator)
            allNumbersWithAnySeparator = true;
        }
        if (!allNumbersWithAnySeparator)
          allNumbersInAllSample = false;
      }
      
      if (numOfSplits[0] != -1 && allNumbersInAllSample) {
        // Each line is comma separated and all values are numbers
        if (numOfSplits[0] == 2) {
          // Point
          autoDetectedShape = "point";
        } else if (numOfSplits[0] == 4) {
          // Rectangle
          autoDetectedShape = "rect";
        }
      } else if (ogcIndex >= 0) {
        // There is a column with geometry stored in it
        this.setInt("column", ogcIndex);
        int numOfColumns = 0;
        for (int iSeparator = 0; iSeparator < Separators.length; iSeparator++) {
          if (numOfSplits[iSeparator] != -1) {
            this.set("separator", Separators[iSeparator]);
            numOfColumns = numOfSplits[iSeparator];
          }
        }
        if (numOfColumns == 1 && ogcIndex == 0)
          autoDetectedShape = OGCJTSShape.class.getName();
        else
          autoDetectedShape = CSVOGC.class.getName();
      }
    } catch (IOException e) {
    }
    if (autoDetectedShape == null) {
      LOG.warn("Could not autodetect shape for input '"+sampleLines.get(0)+"'");
      return false;
    } else {
      LOG.info("Autodetected shape '"+autoDetectedShape+"' for input '"+sampleLines.get(0)+"'");
      this.set("shape", autoDetectedShape);
      return true;
    }
  }
}
