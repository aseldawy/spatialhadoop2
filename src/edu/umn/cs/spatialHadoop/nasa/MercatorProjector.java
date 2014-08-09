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

package edu.umn.cs.spatialHadoop.nasa;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;


/**
 * Projects a NASAPoint in HDF file from Sinusoidal projection to Mercator
 * projection.
 * @author Ahmed Eldawy
 *
 */
public class MercatorProjector implements GeoProjector {
  
  @Override
  public void project(Shape shape) {
    if (shape instanceof Point) {
      Point pt = (Point) shape;
      double oldY=pt.y;
      // Use the Mercator projection to draw an image similar to Google Maps
      // http://stackoverflow.com/questions/14329691/covert-latitude-longitude-point-to-a-pixels-x-y-on-mercator-projection
      double latRad = pt.y * Math.PI / 180.0;
      double mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      pt.y = -180 * mercN / Math.PI;
      
//      System.out.println(oldY+" => "+pt.y);
       
    } else if (shape instanceof Rectangle) {
      Rectangle rect = (Rectangle) shape;
      double oldY=rect.y1;
      double latRad = rect.y1 * Math.PI / 180.0;
      double mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      rect.y1 = -180 * mercN / Math.PI;
      
      latRad = rect.y2 * Math.PI / 180.0;
      mercN = Math.log(Math.tan(Math.PI/4-latRad/2));
      rect.y2 = -180 * mercN / Math.PI;
      
      
      System.out.println(oldY+" => "+rect.y1);
      
    } else {
      throw new RuntimeException("Cannot project shapes of type "+shape.getClass());
    }
  }
}
