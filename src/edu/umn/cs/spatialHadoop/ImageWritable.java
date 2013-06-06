package edu.umn.cs.spatialHadoop;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Writable;

/**
 * An in-memory image that can be used with MapReduce
 * @author eldawy
 *
 */
public class ImageWritable implements Writable {
  /**
   * The underlying image
   */
  private BufferedImage image;
  
  /**
   * A default constructor is necessary to be readable/writable
   */
  public ImageWritable() {
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageIO.write(this.image, "png", baos);
    baos.close();
    byte[] bytes = baos.toByteArray();
    out.write(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    this.image = ImageIO.read(new ByteArrayInputStream(bytes));
  }

  public BufferedImage getImage() {
    return image;
  }
  
  public void setImage(BufferedImage image) {
    this.image = image;
  }
}
