package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.io.Text;

/**
 * Implementing this interface allows objects to be converted easily
 * to and from a string.
 * @author aseldawy
 *
 */
public interface TextSerializable {
  /**
   * Store current object as string in the given text appending text already there.
   * @param text
   */
  public Text toText(Text text);
  
  /**
   * Retrieve information from the given text.
   * @param text
   */
  public void fromText(Text text);
}
