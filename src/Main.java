import java.io.IOException;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;

public class Main {
  public static void main(String[] args) throws IOException {
    Rectangle[] rectangles = new Rectangle[200000];
    for (int i = 0; i < rectangles.length; i++) {
      rectangles[i] = new Rectangle();
      rectangles[i].x1 = rectangles[i].x2 = Math.random();
      rectangles[i].y1 = rectangles[i].y2 = Math.random();
      rectangles[i].x2 += Math.random() / 1000000;
      rectangles[i].y2 += Math.random() / 1000000;
    }
    long t1 = System.currentTimeMillis();
    for (int i1 = 0; i1 < rectangles.length; i1++) {
      for (int i2 = i1+1; i2 < rectangles.length; i2++) {
        if (rectangles[i1].isIntersected(rectangles[i2]));
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Joined "+rectangles.length+" rectangles in "+(t2-t1)+" millis using nested loop");
    
    t1 = System.currentTimeMillis();
    SpatialAlgorithms.SelfJoin_rectangles(rectangles, null, null);
    t2 = System.currentTimeMillis();
    System.out.println("Joined "+rectangles.length+" rectangles in "+(t2-t1)+" millis using planesweep");
  }
}
