package edu.umn.cs.spatialHadoop.visualization;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.io.FileWriter;

import com.esri.core.geometry.Point;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.nasa.GeoProjector;

public abstract class MercatorProjection {

	public static void main(String[] args) {
		
		
		
		BufferedReader buf=null;
		double latRad;
		double mercN;
		BufferedWriter bw = null;
		FileWriter fw = null;
		try{
            buf = new BufferedReader(new FileReader("all_nodes_small.csv"));
           // System.out.println(buf.readLine());
            String line= new String();
            String[] wordsArray;
            File file = new File("AllNodesMercator.csv");
            fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			
			double largestLatitude = 85.051129;
            
            while ((line=buf.readLine())!=null){
            	//System.out.println(line);
            	if(line.length()>0){
            	  String[] words = line.split("\t");
            	  //System.out.println("x="+words[1]+"y="+words[2]);
            	  double latitude = Double.parseDouble(words[2]);
            	  if (Math.abs(latitude) > largestLatitude)
            		  continue;
            	  double longitude = Double.parseDouble(words[1]);
            	  double lambda = longitude * Math.PI / 180; // longitude in radians
            	  double phi = latitude*Math.PI/180; // latitude in radians
            	  double x = 128 / Math.PI * (lambda + Math.PI);
            	  double y = 128 / Math.PI * (Math.PI - Math.log(Math.tan(Math.PI/4+phi/2)));
            	  //mercN = Math.log(Math.tan((Math.PI/4)+(latRad/2)));
            	  //y = -180 * mercN / Math.PI;
            	 
            	  if (!file.exists()) {
      				file.createNewFile();
      			  }
            	  
            	  
      			  String data=words[0]+"\t"+x+"\t"+y+"\t"+words[3];
      			  bw.write(data);
            	  bw.write("\n");
            	}
            	
            }
          //  Rectangle mbr = new Rectangle(0, 0, 256, 256);
            
            
    
		
		}	catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	if (bw != null) {
        		try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
            if (buf != null) {
                try {
                    buf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
}
	