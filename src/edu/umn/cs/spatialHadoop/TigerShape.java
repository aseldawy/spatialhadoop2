package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.JTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A shape from tiger file.
 * @author aseldawy
 *
 */
public class TigerShape extends JTSShape {
  
  @Override
  public void fromText(Text text) {
    byte[] bytes = text.getBytes();
    int i = 0;
    while (i < text.getLength() && bytes[i] != ',') {
      i++;
    }
    text.set(bytes, 0, i);
    super.fromText(text);
  }
}
