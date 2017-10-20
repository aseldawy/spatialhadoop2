package edu.umn.cs.spatialHadoop.visualization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.Writable;

public class GridHistogram implements Writable {
	
	private int width, height;
	private long[] values;

	public GridHistogram() {
	}
	
	public GridHistogram(int width, int height) {
		this.width = width;
		this.height = height;
		values = new long[width * height];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzos = new GZIPOutputStream(baos);
		ByteBuffer bbuffer = ByteBuffer.allocate(getHeight() * 8 + 8);
		bbuffer.putInt(getWidth());
		bbuffer.putInt(getHeight());
		gzos.write(bbuffer.array(), 0, bbuffer.position());
		for (int x = 0; x < getWidth(); x++) {
			bbuffer.clear();
			for (int y = 0; y < getHeight(); y++) {
				bbuffer.putLong(values[y*width + x]);
			}
			gzos.write(bbuffer.array(), 0, bbuffer.position());
		}
		gzos.close();

		byte[] serializedData = baos.toByteArray();
		out.writeInt(serializedData.length);
		out.write(serializedData);
	}
	
	public void set(int x, int y, int size) {
		values[y * width + x] += size;
	}
	
	public void merge(GridHistogram another) {
		if (another.width != this.width || another.height != this.height)
			throw new RuntimeException("Incompatible sizes");
		for (int i = 0; i < values.length; i++) {
			this.values[i] += another.values[i];
		}
	}

	public int getWidth() {
		return width;
	}
	
	public int getHeight() {
		return height;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		byte[] serializedData = new byte[length];
		in.readFully(serializedData);
		ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
		GZIPInputStream gzis = new GZIPInputStream(bais);

		byte[] buffer = new byte[8];
		gzis.read(buffer);
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		int width = bbuffer.getInt();
		int height = bbuffer.getInt();
		// Reallocate memory only if needed
		if (width != this.getWidth() || height != this.getHeight()) {
			values = new long[width*height];
			this.width = width;
			this.height = height;
		}
		buffer = new byte[getHeight() * 8];
		for (int x = 0; x < getWidth(); x++) {
			int size = 0;
			while (size < buffer.length) {
				size += gzis.read(buffer, size, buffer.length - size);
			}
			bbuffer = ByteBuffer.wrap(buffer);
			for (int y = 0; y < getHeight(); y++) {
				values[y*width + x] = bbuffer.getLong();
			}
		}
	}

}
