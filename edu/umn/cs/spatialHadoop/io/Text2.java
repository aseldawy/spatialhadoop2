package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.io.Text;

/**
 * A modified version of Text which is optimized for appends.
 * @author eldawy
 *
 */
public class Text2 extends Text implements TextSerializable {

  public Text2() {
  }

  public Text2(String string) {
    super(string);
  }

  public Text2(Text utf8) {
    super(utf8);
  }

  public Text2(byte[] utf8) {
    super(utf8);
  }

  @Override
  public Text toText(Text text) {
    text.append(getBytes(), 0, getLength());
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.set(text);
  }
}
