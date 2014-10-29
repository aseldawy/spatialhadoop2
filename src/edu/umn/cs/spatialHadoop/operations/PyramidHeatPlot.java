package edu.umn.cs.spatialHadoop.operations;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.PyramidOutputFormat;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASARectangle;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.operations.PyramidPlot.TileIndex;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeFilter;

public class PyramidHeatPlot {
	/** Logger */
	private static final Log LOG = LogFactory.getLog(PyramidHeatPlot.class);

	/** Minimal Bounding Rectangle of input file */
	private static final String InputMBR = "PlotPyramid.InputMBR";
	/** Valid range of values for HDF dataset */
	private static final String MinValue = "plot.min_value";
	private static final String MaxValue = "plot.max_value";
	
	public static class TileIndexAndFrequencyMap implements Writable{
		private FrequencyMap map;
		private TileIndex tileIndex;
		
		public TileIndexAndFrequencyMap(TileIndex tileIndex, FrequencyMap map) {
			this.tileIndex = tileIndex;
			this.map = map;
		}
		
		public TileIndexAndFrequencyMap() {
			tileIndex = new TileIndex();
			map = new FrequencyMap();	
		}
 		
		public TileIndex getTileIndex() {
			return tileIndex;
		}
		
		public FrequencyMap getFrequencyMap() {
			return map;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			tileIndex.readFields(in);
			map.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			tileIndex.write(out);
			map.write(out);
		}
	}
	
	public static class FrequencyMap implements Writable {
		float[][] frequency;
		float[][] gaussianKernel;
		float[][] circleKernel;
		int radius;

		private Map<Integer, BufferedImage> cachedCircles = new HashMap<Integer, BufferedImage>();

		public FrequencyMap() {
		}

		public FrequencyMap(int width, int height) {
			frequency = new float[width][height];
		}

		public FrequencyMap(FrequencyMap other) {
			this.frequency = new float[other.getWidth()][other.getHeight()];
			for (int x = 0; x < this.getWidth(); x++)
				for (int y = 0; y < this.getHeight(); y++) {
					this.frequency[x][y] = other.frequency[x][y];
				}
		}

		public void combine(FrequencyMap other) {
			if (other.getWidth() != this.getWidth()
					|| other.getHeight() != this.getHeight())
				throw new RuntimeException("Incompatible frequency map sizes "
						+ this + ", " + other);
			for (int x = 0; x < this.getWidth(); x++)
				for (int y = 0; y < this.getHeight(); y++) {
					this.frequency[x][y] += other.frequency[x][y];
				}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GZIPOutputStream gzos = new GZIPOutputStream(baos);
			ObjectOutputStream oos = new ObjectOutputStream(gzos);
			oos.writeObject(frequency);
			oos.close();
			byte[] serializedData = baos.toByteArray();
			out.writeInt(serializedData.length);
			out.write(serializedData);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			int length = in.readInt();
			byte[] serializedData = new byte[length];
			in.readFully(serializedData);
			ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
			GZIPInputStream gzis = new GZIPInputStream(bais);
			ObjectInputStream ois = new ObjectInputStream(gzis);
			try {
				frequency = (float[][]) ois.readObject();
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(
						"Could not read the frequency map back from the stream",
						e);
			}
		}

		@Override
		protected FrequencyMap clone() {
			return new FrequencyMap(this);
		}

		@Override
		public boolean equals(Object obj) {
			FrequencyMap other = (FrequencyMap) obj;
			if (this.getWidth() != other.getWidth())
				return false;
			if (this.getHeight() != other.getHeight())
				return false;
			for (int x = 0; x < this.getWidth(); x++) {
				for (int y = 0; y < this.getHeight(); y++) {
					if (this.frequency[x][y] != other.frequency[x][y])
						return false;
				}
			}
			return true;
		}

		@Override
		public String toString() {
			return "Frequency Map:" + this.getWidth() + "x" + this.getHeight();
		}

		private MinMax getValueRange() {
			Map<Float, Integer> histogram = new HashMap<Float, Integer>();
			MinMax minMax = new MinMax(Integer.MAX_VALUE, Integer.MIN_VALUE);
			for (float[] col : frequency)
				for (float value : col) {
					if (!histogram.containsKey(value)) {
						histogram.put(value, 1);
					} else {
						histogram.put(value, histogram.get(value) + 1);
					}
					minMax.expand(value);
				}
			return minMax;
		}

		private int getWidth() {
			return frequency == null ? 0 : frequency.length;
		}

		private int getHeight() {
			return frequency == null ? 0 : frequency[0].length;
		}

		public BufferedImage toImage(MinMax valueRange, boolean skipZeros) {

			if (valueRange == null)
				valueRange = getValueRange();
			LOG.info("Using the value range: " + valueRange);
			NASAPoint.minValue = valueRange.minValue;
			NASAPoint.maxValue = valueRange.maxValue;
			BufferedImage image = new BufferedImage(getWidth(), getHeight(),
					BufferedImage.TYPE_INT_ARGB);
			for (int x = 0; x < this.getWidth(); x++)
				for (int y = 0; y < this.getHeight(); y++) {
					if (!skipZeros || frequency[x][y] > valueRange.minValue) {
						Color color = NASARectangle
								.calculateColor(frequency[x][y]);
						image.setRGB(x, y, color.getRGB());
					}
				}
			return image;
		}

		public void addPoint(int cx, int cy, int radius) {
			BufferedImage circle = getCircle(radius);
			for (int x = 0; x < circle.getWidth(); x++) {
				for (int y = 0; y < circle.getHeight(); y++) {
					int imgx = x - radius + cx;
					int imgy = y - radius + cy;
					if (imgx >= 0 && imgx < getWidth() && imgy >= 0
							&& imgy < getHeight()) {
						frequency[x - radius + cx][y - radius + cy] += circleKernel[x][y];
					}
				}
			}
		}

		// Add a point but updating the distribution in the radius using
		// Gaussian Distribution.
		public void addGaussianPoint(int cx, int cy, int radius) {
			BufferedImage circle = getCircle(radius);
			// Create Gaussian Kernel for the first time.
			if (gaussianKernel == null) {
				createGaussianKernel(radius);
			}

			for (int x = 0; x < circle.getWidth(); x++) {
				for (int y = 0; y < circle.getHeight(); y++) {
					int imgx = x - radius + cx;
					int imgy = y - radius + cy;
					if (imgx >= 0 && imgx < getWidth() && imgy >= 0
							&& imgy < getHeight()) {
						frequency[x - radius + cx][y - radius + cy] += gaussianKernel[x][y];
					}
				}
			}
		}

		// Create the GaussianKernel from a radius and standard deviation.
		public void createGaussianKernel(int radius) {
			float stdev = 8;
			gaussianKernel = new float[2 * radius + 1][2 * radius + 1];
			float temp = 0;

			// Create the gaussianKernel.
			for (int j = 0; j < gaussianKernel.length; j++) {
				for (int i = 0; i < gaussianKernel.length; i++) {
					// If this is the center of the kernel.
					temp = (float) Math.pow(
							Math.E,
							(-1 / (2 * Math.pow(stdev, 2)))
									* (Math.pow(Math.abs(radius - i), 2) + Math
											.pow(Math.abs(radius - j), 2)));
					gaussianKernel[i][j] = temp;
				}
			}
		}

		// Get the GaussianKernel
		public float[][] getGaussiankernel(int radius) {
			if (gaussianKernel == null) {
				createGaussianKernel(radius);
			}

			return gaussianKernel;
		}

		public BufferedImage getCircle(int radius) {
			BufferedImage circle = cachedCircles.get(radius);
			if (circle == null) {
				circle = new BufferedImage(radius * 2, radius * 2,
						BufferedImage.TYPE_INT_RGB);
				Graphics2D graphics = circle.createGraphics();
				graphics.setBackground(Color.WHITE);
				graphics.clearRect(0, 0, radius * 2, radius * 2);
				graphics.setColor(Color.BLACK);
				graphics.fillArc(0, 0, radius * 2, radius * 2, 0, 360);
				graphics.dispose();
				cachedCircles.put(radius, circle);
				createCircleKernel(radius);
			}
			return circle;
		}

		public void createCircleKernel(int radius) {
			circleKernel = new float[2 * radius][2 * radius];
			BufferedImage circle = cachedCircles.get(radius);
			for (int x = 0; x < circleKernel.length; x++) {
				for (int y = 0; y < circleKernel[0].length; y++) {
					boolean filled = (circle.getRGB(x, y) & 0xff) == 0;
					if (filled) {
						circleKernel[x][y] = 1;
					} else {
						circleKernel[x][y] = 0;
					}
				}
			}
		}
	}

	/**
	 * The map function replicates each object to all the tiles it overlaps
	 * with. It starts with the bottom level and replicates each shape to all
	 * the levels up to the top of the pyramid.
	 * 
	 * @author Ahmed Eldawy
	 */
	public static class SpacePartitionMap extends MapReduceBase implements
			Mapper<Rectangle, ShapeIterator, TileIndex, Point> {

		/** Number of levels in the pyramid */
		private int topLevel;
		private int bottomLevel;
		/** The grid at the bottom level of the pyramid */
		private GridInfo bottomGrid;
		private int tileWidth;
		private int tileHeight;
		/** Used as a key for output */
		private TileIndex key;
		/** The probability of replicating a point to level i */
		private double[] levelProb;
		private double[] scale;
		private boolean adaptiveSampling;
		private Rectangle fileMBR;

		/** Heat Map Stuffs */
		private double radius;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			this.radius = job.getInt("radius", 30);
			this.topLevel = job.getInt("topLevel", 1);
			this.bottomLevel = job.getInt("bottomLevel", 0);
			this.fileMBR = SpatialSite.getRectangle(job, InputMBR);
			this.bottomGrid = new GridInfo(fileMBR.x1, fileMBR.y1, fileMBR.x2,
					fileMBR.y2);
			this.bottomGrid.rows = bottomGrid.columns = (int) Math.round(Math
					.pow(2, topLevel));
			this.key = new TileIndex();
			this.adaptiveSampling = job.getBoolean("sample", false);
			this.levelProb = new double[this.topLevel + 1];
			this.scale = new double[topLevel + 1];
			this.tileWidth = job.getInt("tilewidth", 256);
			this.tileHeight = job.getInt("tileheight", 256);
			this.scale[0] = Math.sqrt((double) tileWidth * tileHeight
					/ (fileMBR.getWidth() * fileMBR.getHeight()));
			this.levelProb[0] = job.getFloat(GeometricPlot.AdaptiveSampleRatio,
					0.1f);
			for (int level = 1; level < topLevel; level++) {
				this.levelProb[level] = this.levelProb[level - 1] * 4;
				this.scale[level] = this.scale[level - 1] * (1 << level);
			}
		}

		public void map(Rectangle cell, ShapeIterator value,
				OutputCollector<TileIndex, Point> output, Reporter reporter)
				throws IOException {

			while (value.hasNext()) {

				Shape shape = value.next();
				
				Point center;
				if (shape instanceof Point) {
					center = new Point((Point)shape);
				} else if (shape instanceof Rectangle) {
					center = new Point(((Rectangle) shape).getCenterPoint());
				} else {
					Rectangle shapeMBR = shape.getMBR();
					if (shapeMBR == null)
						continue;
					center = new Point(((Rectangle) shape).getCenterPoint());
				}

				Rectangle shapeMBR = center.getMBR().buffer(radius, radius);
				int min_level = bottomLevel;

				if (adaptiveSampling) {
					// Special handling for NASA data
					double p = Math.random();
					// Skip levels that do not satisfy the probability
					while (min_level <= topLevel && p > levelProb[min_level])
						min_level++;
				}

				java.awt.Rectangle overlappingCells = bottomGrid
						.getOverlappingCells(shapeMBR);

				for (key.level = topLevel; key.level >= min_level; key.level--) {
					for (int i = 0; i < overlappingCells.width; i++) {
						key.x = i + overlappingCells.x;
						for (int j = 0; j < overlappingCells.height; j++) {
							key.y = j + overlappingCells.y;
							// Output the point
							output.collect(key, center);
						}
					}
					// Shrink overlapping cells to match the upper level
					int updatedX1 = overlappingCells.x / 2;
					int updatedY1 = overlappingCells.y / 2;
					int updatedX2 = (overlappingCells.x
							+ overlappingCells.width - 1) / 2;
					int updatedY2 = (overlappingCells.y
							+ overlappingCells.height - 1) / 2;
					overlappingCells.x = updatedX1;
					overlappingCells.y = updatedY1;
					overlappingCells.width = updatedX2 - updatedX1 + 1;
					overlappingCells.height = updatedY2 - updatedY1 + 1;
				}
				reporter.progress();
			}
		}
	}

	/**
	 * The reducer class draws an image for contents (shapes) in each tile
	 * 
	 * @author Ahmed Eldawy
	 *
	 */
	public static class SpacePartitionReduce extends MapReduceBase implements
			Reducer<TileIndex, Point, TileIndex, ImageWritable> {

		private Rectangle fileMBR;
		private int tileWidth, tileHeight;
		private double radius;
		private boolean smooth;
		private FrequencyMap map;
		private boolean skipZeros;
		private MinMax valueRange;

		@Override
		public void configure(JobConf job) {
			System.setProperty("java.awt.headless", "true");
			super.configure(job);
			fileMBR = SpatialSite.getRectangle(job, InputMBR);
			tileWidth = job.getInt("tilewidth", 256);
			tileHeight = job.getInt("tileheight", 256);
			NASAPoint.minValue = job.getInt(MinValue, 0);
			NASAPoint.maxValue = job.getInt(MaxValue, 65535);
			NASAPoint.setColor1(OperationsParams.getColor(job, "color1",
					Color.BLUE));
			NASAPoint.setColor2(OperationsParams.getColor(job, "color2",
					Color.RED));
			NASAPoint.gradientType = OperationsParams.getGradientType(job,
					"gradient", NASAPoint.GradientType.GT_HUE);

			this.radius = job.getInt("radius", 30);
			this.smooth = job.getBoolean("smooth", false);
			this.map = new FrequencyMap(tileWidth, tileHeight);
			this.skipZeros = job.getBoolean("skipzeros", false);
			String valueRangeStr = job.get("valuerange");
			if (valueRangeStr != null) {
				String[] parts = valueRangeStr.contains("..") ? valueRangeStr
						.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
				this.valueRange = new MinMax(Integer.parseInt(parts[0]),
						Integer.parseInt(parts[1]));
			}
		}

		@Override
		public void reduce(TileIndex tileIndex, Iterator<Point> values,
				OutputCollector<TileIndex, ImageWritable> output,
				Reporter reporter) throws IOException {

			// Find the width and height of a tile from the
			// input space.
			double currentTileWidth = fileMBR.getWidth()
					/ Math.pow(2, tileIndex.level);
			double currentTileHeight = fileMBR.getHeight()
					/ Math.pow(2, tileIndex.level);

			// Find the location of a tile of a input space.
			double xTile = (tileIndex.x * currentTileWidth) + fileMBR.x1;
			double yTile = (tileIndex.y * currentTileHeight) + fileMBR.y1;

			while (values.hasNext()) {
				Point shape = values.next();
				
				// Get the center and put into frequency map.
				int centerx = (int) Math.round((shape.x - xTile) * tileWidth
						/ currentTileWidth);
				int centery = (int) Math.round((shape.y - yTile) * tileHeight
						/ currentTileHeight);
				
				double scale = Math.sqrt((double) tileWidth * tileHeight
						/ (fileMBR.getWidth() * fileMBR.getHeight()));
				double currentRadius = radius / scale;
				// Add the point to current FrequencyMap.
				if (smooth) {
					map.addGaussianPoint(centerx, centery, (int) Math.ceil(currentRadius));
				} else {
					map.addPoint(centerx, centery, (int) Math.ceil(radius));
				}
			}
			BufferedImage image = map.toImage(valueRange, skipZeros);
			output.collect(tileIndex, new ImageWritable(image));
		}
	}

	public static class DataPartitionMap extends MapReduceBase implements
			Mapper<Rectangle, ShapeIterator, TileIndex, FrequencyMap> {

		/** Number of levels in the pyramid */
		private int topLevel;
		private int bottomLevel;
		/** The grid at the bottom level of the pyramid */
		private GridInfo bottomGrid;
		/** Used as a key for output */
		private TileIndex key;
		/** The probability of replicating a point to level i */
		private float[] levelProb;
		private double scale[];
		/** Whether to use adaptive sampling with points or not */
		private boolean adaptiveSampling;
		/** Width of each tile in pixels */
		private int tileWidth;
		/** Height of each tile in pixels */
		private int tileHeight;

		/** Heat Map Stuffs */
		private Map<TileIndex, FrequencyMap> tileMap = new HashMap<TileIndex, FrequencyMap>();
		private double radius;
		private boolean smooth;

		private Rectangle fileMBR;

		@Override
		public void configure(JobConf job) {
			System.setProperty("java.awt.headless", "true");
			super.configure(job);

			this.radius = job.getInt("radius", 30);
			this.smooth = job.getBoolean("smooth", false);
			this.tileWidth = job.getInt("tilewidth", 256);
			this.tileHeight = job.getInt("tileheight", 256);
			this.topLevel = job.getInt("topLevel", 1);
			this.bottomLevel = job.getInt("bottomLevel", 0);
			this.fileMBR = SpatialSite.getRectangle(job, InputMBR);
			this.bottomGrid = new GridInfo(fileMBR.x1, fileMBR.y1, fileMBR.x2,
					fileMBR.y2);
			this.bottomGrid.rows = bottomGrid.columns = (int) Math.round(Math
					.pow(2, topLevel));
			this.key = new TileIndex();
			this.adaptiveSampling = job.getBoolean("sample", false);
			this.levelProb = new float[this.topLevel + 1];
			this.scale = new double[topLevel + 1];
			this.levelProb[0] = job.getFloat(GeometricPlot.AdaptiveSampleRatio,
					0.1f);
			this.scale[0] = Math.sqrt((double) tileWidth * tileHeight
					/ (fileMBR.getWidth() * fileMBR.getHeight()));
			for (int level = 1; level <= topLevel; level++) {
				this.levelProb[level] = this.levelProb[level - 1] * 4;
				this.scale[level] = this.scale[level - 1] * (1 << level);
			}
		}

		@Override
		public void map(Rectangle d, ShapeIterator value,
				OutputCollector<TileIndex, FrequencyMap> output,
				Reporter reporter) throws IOException {

			while (value.hasNext()) {
				
				Shape shape = value.next();
				Point center;
				if (shape instanceof Point) {
					center = (Point) shape;
				} else if (shape instanceof Rectangle) {
					center = ((Rectangle) shape).getCenterPoint();
				} else {
					Rectangle shapeMBR = shape.getMBR();
					if (shapeMBR == null)
						continue;
					center = shapeMBR.getCenterPoint();
				}
				Rectangle shapeMBR = center.getMBR().buffer(radius, radius);
				int min_level = bottomLevel;

				if (adaptiveSampling) {
					// Special handling for NASA data
					double p = Math.random();
					// Skip levels that do not satisfy the probability
					while (min_level <= topLevel && p > levelProb[min_level])
						min_level++;
				}

				java.awt.Rectangle overlappingCells = bottomGrid
						.getOverlappingCells(shapeMBR);

				for (key.level = topLevel; key.level >= min_level; key.level--) {
					for (int i = 0; i < overlappingCells.width; i++) {
						key.x = i + overlappingCells.x;
						for (int j = 0; j < overlappingCells.height; j++) {
							key.y = j + overlappingCells.y;

							// Find the width and height of a tile from the
							// input space.
							double currentTileWidth = fileMBR.getWidth()
									/ Math.pow(2, key.level);
							double currentTileHeight = fileMBR.getHeight()
									/ Math.pow(2, key.level);

							// Find the location of a tile of a input space.
							double xTile = (key.x * currentTileWidth)
									+ fileMBR.x1;
							double yTile = (key.y * currentTileHeight)
									+ fileMBR.y1;

							// Get the center and put into frequency map.
							int centerx = (int) Math.round((center.x - xTile)
									* tileWidth / currentTileWidth);
							int centery = (int) Math.round((center.y - yTile)
									* tileHeight / currentTileHeight);
							
							double currentRadius = radius / scale[key.level];
							// Update the Map.
							setMap(key, centerx, centery, currentRadius, smooth);
						}
					}
					// Shrink overlapping cells to match the upper level
					int updatedX1 = overlappingCells.x / 2;
					int updatedY1 = overlappingCells.y / 2;
					int updatedX2 = (overlappingCells.x
							+ overlappingCells.width - 1) / 2;
					int updatedY2 = (overlappingCells.y
							+ overlappingCells.height - 1) / 2;
					overlappingCells.x = updatedX1;
					overlappingCells.y = updatedY1;
					overlappingCells.width = updatedX2 - updatedX1 + 1;
					overlappingCells.height = updatedY2 - updatedY1 + 1;
				}
				reporter.progress();
			}

			// Emit the result
			for (Map.Entry<TileIndex, FrequencyMap> entry : tileMap.entrySet()) {
				output.collect(entry.getKey(), entry.getValue());
			}
		}

		// Insert the record to the correct FrequencyMap based on tile
		private void setMap(TileIndex tileIndex, int cx, int cy, double radius,
				boolean smooth) {

			FrequencyMap map = tileMap.get(tileIndex);
			if (map == null) {
				tileIndex = tileIndex.clone();
				map = new FrequencyMap(tileWidth, tileHeight);
			}
			if (smooth) {
				map.addGaussianPoint(cx, cy, (int) Math.ceil(radius));
			} else {
				map.addPoint(cx, cy, (int) Math.ceil(radius));
			}
			tileMap.put(tileIndex, map);
		}
	}

	public static class DataPartitionReduce extends MapReduceBase implements
			Reducer<TileIndex, FrequencyMap, TileIndex, ImageWritable> {

		/** Range of values to do the gradient of the heat map */
		private MinMax valueRange;
		private boolean skipZeros;

		@Override
		public void configure(JobConf job) {
			System.setProperty("java.awt.headless", "true");
			super.configure(job);

			NASAPoint.setColor1(OperationsParams.getColor(job, "color1",
					new Color(0, 0, 255, 0)));
			NASAPoint.setColor2(OperationsParams.getColor(job, "color2",
					new Color(255, 0, 0, 255)));
			NASAPoint.gradientType = OperationsParams.getGradientType(job,
					"gradient", NASAPoint.GradientType.GT_HUE);
			this.skipZeros = job.getBoolean("skipzeros", false);
			String valueRangeStr = job.get("valuerange");
			if (valueRangeStr != null) {
				String[] parts = valueRangeStr.contains("..") ? valueRangeStr
						.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
				this.valueRange = new MinMax(Integer.parseInt(parts[0]),
						Integer.parseInt(parts[1]));
			}
		}

		@Override
		public void reduce(TileIndex cellIndex, Iterator<FrequencyMap> maps,
				OutputCollector<TileIndex, ImageWritable> output,
				Reporter reporter) throws IOException {

			FrequencyMap combined = null;
			while (maps.hasNext()) {
				FrequencyMap temp = maps.next();

				if (combined == null) {
					combined = temp.clone();
				} else {
					combined.combine(temp);
				}
			}

			BufferedImage image = combined.toImage(valueRange, skipZeros);
			output.collect(cellIndex, new ImageWritable(image));
		}
	}

	/**
	 * Finalizes the job by combining all images of all reduces jobs into one
	 * folder.
	 * 
	 * @author Ahmed Eldawy
	 *
	 */
	public static class PlotPyramidOutputCommitter extends FileOutputCommitter {
		@Override
		public void commitJob(JobContext context) throws IOException {
			super.commitJob(context);

			JobConf job = context.getJobConf();
			Path outPath = PyramidOutputFormat.getOutputPath(job);
			FileSystem outFs = outPath.getFileSystem(job);

			// Write a default empty image to be displayed for non-generated
			// tiles
			int tileWidth = job.getInt("tilewidth", 256);
			int tileHeight = job.getInt("tileheight", 256);
			BufferedImage emptyImg = new BufferedImage(tileWidth, tileHeight,
					BufferedImage.TYPE_INT_ARGB);
			Graphics2D g = new SimpleGraphics(emptyImg);
			g.setBackground(new Color(0, 0, 0, 0));
			g.clearRect(0, 0, tileWidth, tileHeight);
			g.dispose();

			OutputStream out = outFs.create(new Path(outPath, "default.png"));
			ImageIO.write(emptyImg, "png", out);
			out.close();

			// Get the correct levels.
			String levelsRange = job.get("levels", "0,7");
			int lowerRange = 0;
			int upperRange = 1;
			if (levelsRange != null) {
				if (!levelsRange.contains(",")) {
					upperRange = Integer.parseInt(levelsRange);
					// For levels not start from 0.
				} else {
					String[] parts = levelsRange.split(",", 2);
					lowerRange = Integer.parseInt(parts[0]);
					upperRange = Integer.parseInt(parts[1]);
				}
			}

			// Add an HTML file that visualizes the result using Google Maps
			int bottomLevel = lowerRange;
			int topLevel = upperRange;
			LineReader templateFileReader = new LineReader(getClass()
					.getResourceAsStream("/zoom_view.html"));
			PrintStream htmlOut = new PrintStream(outFs.create(new Path(
					outPath, "index.html")));
			Text line = new Text();
			while (templateFileReader.readLine(line) > 0) {
				String lineStr = line.toString();
				lineStr = lineStr.replace("#{TILE_WIDTH}",
						Integer.toString(tileWidth));
				lineStr = lineStr.replace("#{TILE_HEIGHT}",
						Integer.toString(tileHeight));
				lineStr = lineStr.replace("#{MAX_ZOOM}",
						Integer.toString(topLevel));
				lineStr = lineStr.replace("#{MIN_ZOOM}",
						Integer.toString(bottomLevel));

				htmlOut.println(lineStr);
			}
			templateFileReader.close();
			htmlOut.close();
		}
	}

	/** Last submitted job of type PlotPyramid */
	public static RunningJob lastSubmittedJob;

	private static <S extends Shape> RunningJob plotMapReduce(Path inFile,
			Path outFile, OperationsParams params) throws IOException {

		String hdfDataset = (String) params.get("dataset");
		Shape shape = hdfDataset != null ? new NASARectangle() : params
				.getShape("shape");
		Shape plotRange = params.getShape("rect");

		boolean background = params.is("background");

		JobConf job = new JobConf(params, PyramidHeatPlot.class);
		job.setJobName("HeatMapPlotPyramid");

		String partition = job.get("partition", "space").toLowerCase();
		if (partition.equals("space")) {
			job.setMapperClass(SpacePartitionMap.class);
			job.setReducerClass(SpacePartitionReduce.class);
			job.setMapOutputKeyClass(TileIndex.class);
			job.setMapOutputValueClass(Point.class);
			job.setInputFormat(edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat.class);
		} else {
			LOG.info("Plot using Data Partitioning");
			job.setMapperClass(DataPartitionMap.class);
			job.setReducerClass(DataPartitionReduce.class);
			job.setMapOutputKeyClass(TileIndex.class);
			job.setMapOutputValueClass(FrequencyMap.class);
			job.setInputFormat(edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat.class);
		}

		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
		job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

		if (shape instanceof Point && job.getBoolean("sample", false)) {
			// Enable adaptive sampling
			int imageWidthRoot = job.getInt("tilewidth", 256);
			int imageHeightRoot = job.getInt("tileheight", 256);
			long recordCount = FileMBR.fileMBR(inFile, params).recordCount;
			float sampleRatio = params.getFloat(
					GeometricPlot.AdaptiveSampleFactor, 1.0f)
					* imageWidthRoot
					* imageHeightRoot / recordCount;
			job.setFloat(GeometricPlot.AdaptiveSampleRatio, sampleRatio);
		}

		Rectangle fileMBR;
		if (hdfDataset != null) {
			// Input is HDF
			job.set(HDFRecordReader.DatasetName, hdfDataset);
			job.setBoolean(HDFRecordReader.SkipFillValue, true);
			job.setClass("shape", NASARectangle.class, Shape.class);
			// Determine the range of values by opening one of the HDF files
			Aggregate.MinMax minMax = Aggregate.aggregate(
					new Path[] { inFile }, params);
			job.setInt(MinValue, minMax.minValue);
			job.setInt(MaxValue, minMax.maxValue);
			// fileMBR = new Rectangle(-180, -90, 180, 90);
			fileMBR = plotRange != null ? plotRange.getMBR() : new Rectangle(
					-180, -140, 180, 169);
			// job.setClass(HDFRecordReader.ProjectorClass,
			// MercatorProjector.class,
			// GeoProjector.class);
		} else {
			fileMBR = FileMBR.fileMBR(inFile, params);
		}

		boolean keepAspectRatio = params.is("keep-ratio", true);
		if (keepAspectRatio) {
			// Expand input file to a rectangle for compatibility with the
			// pyramid
			// structure
			if (fileMBR.getWidth() > fileMBR.getHeight()) {
				fileMBR.y1 -= (fileMBR.getWidth() - fileMBR.getHeight()) / 2;
				fileMBR.y2 = fileMBR.y1 + fileMBR.getWidth();
			} else {
				fileMBR.x1 -= (fileMBR.getHeight() - fileMBR.getWidth() / 2);
				fileMBR.x2 = fileMBR.x1 + fileMBR.getHeight();
			}
		}

		SpatialSite.setRectangle(job, InputMBR, fileMBR);

		// Set input and output
		ShapeInputFormat.addInputPath(job, inFile);
		if (plotRange != null) {
			job.setClass(SpatialSite.FilterClass, RangeFilter.class,
					BlockFilter.class);
		}

		job.setOutputFormat(PyramidOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outFile);
		job.setOutputCommitter(PlotPyramidOutputCommitter.class);

		if (OperationsParams.isLocal(job, inFile)) {
			// Enforce local execution if explicitly set by user or for small
			// files
			job.set("mapred.job.tracker", "local");
		}

		if (background) {
			JobClient jc = new JobClient(job);
			return lastSubmittedJob = jc.submitJob(job);
		} else {
			return lastSubmittedJob = JobClient.runJob(job);
		}

	}

	public static <S extends Shape> void plot(Path inFile, Path outFile,
			OperationsParams params) throws IOException {

		String partitionStr = params.get("partition");
		if (partitionStr == null) {
			partitionStr = new String("hybrid");
		}
		partitionStr.toLowerCase();
		// Get the Levels.
		int dataPartitioningMaxLevel = params.getInt("datamaxlevel", 5);
		String levelsRange = params.get("levels");
		int lowerRange = 0;
		int upperRange = 1;
		if (levelsRange != null) {
			if (!levelsRange.contains(",")) {
				upperRange = Integer.parseInt(levelsRange);
				// For levels not start from 0.
			} else {
				String[] parts = levelsRange.split(",", 2);
				lowerRange = Integer.parseInt(parts[0]);
				upperRange = Integer.parseInt(parts[1]);
			}
		}
		if (!partitionStr.equals("hybrid")) {
			params.setInt("bottomLevel", lowerRange);
			params.setInt("topLevel", upperRange);
			plotMapReduce(inFile, outFile, params);
		} else {
			// Set the Levels to a correct job.
			if (upperRange <= dataPartitioningMaxLevel) {
				// Run the Data Partitioning MapReduce until level
				// dataPartitioningMaxLevel.
				params.setInt("bottomLevel", lowerRange);
				params.setInt("topLevel", upperRange);
				params.setStrings("partition", "data");
				plotMapReduce(inFile, outFile, params);
			} else if (lowerRange > dataPartitioningMaxLevel) {
				// Run only Space Partitioning MapReduce
				params.setInt("bottomLevel", lowerRange);
				params.setInt("topLevel", upperRange);
				params.setStrings("partition", "space");
				plotMapReduce(inFile, outFile, params);
			} else {
				// Run the combination of Data Partitioning and Space
				// Partitioning.
				// Run the data first.
				String originalOutputPath = params.getOutputPath().toString();
				String tempOutputPath = originalOutputPath + "_temp";
				params.setOutputPath(tempOutputPath);
				params.setInt("bottomLevel", lowerRange);
				params.setInt("topLevel", dataPartitioningMaxLevel);
				params.setStrings("partition", "data");
				plotMapReduce(inFile, new Path(tempOutputPath), params);

				// Run the space partitioning next.
				params.setOutputPath(originalOutputPath);
				params.setInt("bottomLevel", dataPartitioningMaxLevel + 1);
				params.setInt("topLevel", upperRange);
				params.setStrings("partition", "space");
				plotMapReduce(inFile, outFile, params);

				// Move the files from temporary directory to the original
				// directory.
				FileSystem fs = FileSystem.get(new Configuration());
				FileStatus[] status = fs.listStatus(new Path(tempOutputPath));
				for (FileStatus file : status) {
					if (!file.isDir()) {
						// Check the name of the file and move it if it contains
						// tile.
						String filePath = file.getPath().toString();
						if (filePath.contains("tile_")) {
							String[] fileSplit = filePath.split("/");
							String fileName = fileSplit[fileSplit.length - 1];
							fs.rename(file.getPath(), new Path(
									originalOutputPath + "/" + fileName));
						}
					}
				}
				// Remove the temporary directory files.
				fs.delete(new Path(tempOutputPath), true);
				fs.close();
			}
		}
	}

	private static void printUsage() {
		System.out
				.println("Plots a file to a set of images that form a pyramid to be used by Google Maps or a similar engine");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> - (*) Path to output file");
		System.out
				.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
		System.out
				.println("partition:<data|shape|hybrid> - (*) Type of partition");
		System.out.println("tilewidth:<w> - Width of each tile in pixels");
		System.out.println("tileheight:<h> - Height of each tile in pixels");
		System.out
				.println("levels:<min,max> - Number of levels in the pyrmaid");
		System.out.println("radius:<r> - The radius of the heatmap");
		System.out
				.println("valuerange:<min,max> - Range of values for plotting the heat map");
		System.out.println("color1:<c> - Color to use for minimum values");
		System.out.println("color2:<c> - Color to use for maximum values");
		System.out
				.println("gradient:<hue|color> - Method to change gradient from color1 to color2");
		System.out.println("-overwrite: Override output file without notice");
		System.out
				.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
		System.out
				.println("-skipzeros: Leave empty areas (frequency < min) transparent");
		System.out.println("-smooth: Use Gaussian Distribution");
		System.out.println("datamaxlevel:<d>: The maximum level to use Data Partitioning");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		System.setProperty("java.awt.headless", "true");
		OperationsParams cla = new OperationsParams(new GenericOptionsParser(
				args));
		if (!cla.checkInputOutput()) {
			printUsage();
			return;
		}
		Path inFile = cla.getInputPath();
		Path outFile = cla.getOutputPath();
		long t1 = System.currentTimeMillis();
		plot(inFile, outFile, cla);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time for plot pyramid " + (t2 - t1)
				+ " millis");
	}

}
