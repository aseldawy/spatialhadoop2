
package edu.umn.cs.spatialHadoop.visualization;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

public class ImagePlot {
	double y1 = 0;
	 double y2 = 0;
	 double x1 = 0;
	 double x2 = 0;
	 boolean vflip = true;
	 String Shape = "osm";
	 TileIndex tileIndex;
	 
	 
	 public static ArrayList<Partition> getPartitions(Path masterPath) throws IOException {

			ArrayList<Partition> partitions = new ArrayList<Partition>();

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Text tempLine = new Text2();
			@SuppressWarnings("resource")
			LineReader in = new LineReader(fs.open(masterPath));
		//	System.out.println("Printing  the partitions");
			while (in.readLine(tempLine) > 0) {
				//System.out.println("In: "+tempLine);
				Partition tempPartition = new Partition();
				tempPartition.fromText(tempLine);
				partitions.add(tempPartition);
			}

			return partitions;
		}
		 
	
	public void createImage(String dirName, String inFileName, DataOutputStream output, Boolean upLevel, String newFileName,int zoom_level,int column,int row) throws IOException{
	
		
		Path infile=new Path(dirName+"/"+inFileName);
		//System.out.println("Infile:"+infile);
		//ArrayList <Partition> MasterPartition=getPartitions(infile);
		BufferedReader br = null;
		FileReader fr = null;

		try {

			fr = new FileReader(dirName+"//Configuration.txt");
			br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				String[] strarr = sCurrentLine.split("=");
				if(strarr[0].equals("x1")) x1 = Double.parseDouble(strarr[1]);
				if(strarr[0].equals("x2")) x2 = Double.parseDouble(strarr[1]);
				if(strarr[0].equals("y1")) y1 = Double.parseDouble(strarr[1]);
				if(strarr[0].equals("y2")) y2 = Double.parseDouble(strarr[1]);
				if(strarr[0].equals("vflip")) vflip = Boolean.parseBoolean(strarr[1]);
				if(strarr[0].equals("Shape")) Shape = strarr[1];
			}

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}
		//inFileName = dirName+"//"+inFileName;
		Rectangle inputMBR=new Rectangle(x1,y1,x2,y2);
		long tileID = TileIndex.encode(zoom_level, column, row);
		 
		 
		 tileIndex = TileIndex.decode(tileID, tileIndex);
		 
		 
		 
		 if (vflip)
            tileIndex.y = ((1 << tileIndex.z) - 1) - tileIndex.y;
		 
		 //Rectangle inputMBR = new Rectangle(x1, y1, x2, y2);
		// Rectangle tileMBR =  new Rectangle();
//		int gridSize = 1 << tileIndex.level;
//		tileMBR.x1 = (inputMBR.x1 * (gridSize - tileIndex.x) + inputMBR.x2 * tileIndex.x)
//				/ gridSize;
//		tileMBR.x2 = (inputMBR.x1 * (gridSize - (tileIndex.x + 1))
//				+ inputMBR.x2 * (tileIndex.x + 1)) / gridSize;
//		tileMBR.y1 = (inputMBR.y1 * (gridSize - tileIndex.y) + inputMBR.y2 * tileIndex.y)
//				/ gridSize;
//		tileMBR.y2 = (inputMBR.y1 * (gridSize - (tileIndex.y + 1))
//				+ inputMBR.y2 * (tileIndex.y + 1)) / gridSize;
		//Text fileName=null;
		Rectangle tileMBR=TileIndex.getMBR(inputMBR, tileIndex.z, tileIndex.x, tileIndex.y);
/*		System.out.println("TileMBR:"+tileMBR);
		System.out.println("Size: "+MasterPartition.size()); 
		String file=null;
		for (int i=0;i<MasterPartition.size();i++)
		{
			
			if(MasterPartition.get(i).contains(tileMBR)){
				System.out.println("MAsterPartition:"+MasterPartition.get(i).filename);
				file=MasterPartition.get(i).filename;
				break;
		     
			}
		
		}*/
		//String file=fileName.toString();
		//System.out.println("Filex:"+file);
		  OperationsParams params = new OperationsParams();
	      params.setBoolean("local", true);
	      OperationsParams.setShape(params, "mbr", tileMBR);
	      OperationsParams.setShape(params, SpatialInputFormat3.InputQueryRange, tileMBR);
	      params.set("shape", Shape);
	      params.setBoolean("overwrite", true);
	      params.setBoolean("vflip", vflip);
	      params.setBoolean("keepratio", false);
	      params.setInt("width", 256);
	      params.setInt("height", 256);
	      
		 //Canvas canvasImage = null; 
	    try {
			 SingleLevelPlot.plotLocal(new Path[] { new Path("all_nodes_index") },
			        output, GeometricPlot.GeometricRasterizer.class, params);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
	
	public Rectangle mbrCalc(){
		return null;
	}

}

//x1=-179.147236	x2=179.77847
//y1=-14.548699		y2=71.359879
//height = 85.908578		width = 358.925706


//write to https response
//plugin code
