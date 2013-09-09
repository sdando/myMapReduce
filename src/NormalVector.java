
import java.io.*;
public class NormalVector {

	/**
	 * @param args inputFile,row,column,only trans or norm
	 */
	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		int row=Integer.parseInt(args[1]);
		int column=Integer.parseInt(args[2]);
		int bNorm=Integer.parseInt(args[3]);
	    FileInputStream fis=new FileInputStream(args[0]);
	    InputStreamReader isr=new InputStreamReader(fis);
	    BufferedReader br = new BufferedReader(isr);
	    String line="";
	    String[][] matrix=new String[row][];
	    String[][] transMatrix=new String[column][];
	    for(int n=0;n<column;n++)
	    	transMatrix[n]=new String[row];
	    int m,i=0;
	    while ((line=br.readLine())!=null) {
	        matrix[i++]=line.split(",");
	   }	    
	   for(int j=0;j<row;j++)
		   for(int k=0;k<column;k++){
               transMatrix[k][j]=matrix[j][k];
		   }
	   float norm;
	   if(bNorm==1){
       FileOutputStream fos=new FileOutputStream("normsTrans.txt",true);
       OutputStreamWriter osw=new OutputStreamWriter(fos);
       BufferedWriter bw=new BufferedWriter(osw);
	   
	   float sum,mean,std;
	   for(i=0;i<column;i++){
		   sum=0;
		   for(m=0;m<row;m++){
			   sum=sum+Float.parseFloat(transMatrix[i][m]);
		   }
		   mean=sum/row;
		   std=0;
		   for(m=0;m<row;m++){
			   std=std+(float)Math.pow(Float.parseFloat(transMatrix[i][m])-mean, 2);
		   }
		   std=std/row;
		   std=(float)Math.pow(std, 0.5);
		   StringBuilder outBuilder=new StringBuilder();
		   for(m=0;m<row;m++){
			   if(std!=0){
			   norm=(Float.parseFloat(transMatrix[i][m])-mean)/std;
			   }
			   else{
			   norm=Float.parseFloat(transMatrix[i][m])-mean;   
			   }
			   outBuilder.append(norm);
			   if(m!=row-1)
			      outBuilder.append(",");
		   }
		   bw.write(outBuilder.toString()+"\n");
	   }
       bw.close();
       osw.close();
       fos.close();
	   System.out.println("end");
	}
	else{
		FileOutputStream fos = new FileOutputStream("norms.txt", true);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
	    for(i=0;i<column;i++){
	    	StringBuilder outBuilder=new StringBuilder();
	    	for(m=0;m<row;m++){
	    		norm=Float.parseFloat(transMatrix[i][m]);
				outBuilder.append(norm);
				if(m!=row-1)
				    outBuilder.append(",");
			    }
			   bw.write(outBuilder.toString()+"\n");
	    }				
	    bw.close();
	    osw.close();
	    fos.close();
		System.out.println("end");	
	}   
	}
}
