package KMeans;

import java.util.ArrayList;

public class DmRecord {
    ArrayList<Double> vector;
    private double squaredDistance;     //距离的平方
   
    public DmRecord(){
       
    }
   
    public DmRecord(ArrayList<Double> vector){
    	this.vector=vector;
    }
   
    public double getDis(){
    	return squaredDistance;
    }
    
    public double squaredDistance(DmRecord record){
    	double distance=0.0;
    	for(int i=0;i<this.vector.size();i++){
    		distance+=Math.pow(this.vector.get(i)-record.vector.get(i), 2);
    	}
        return Math.sqrt(distance);
    }
    
    public int compareTo(DmRecord record,double delta){
    	int flag=0;
    	for(int i=0;i<vector.size();i++){
    		if(Math.abs(vector.get(i)-record.vector.get(i))>delta){
    			flag=1;
    			break;
    		}
    	}
    	return flag;
    }
}
