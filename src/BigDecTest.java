import java.math.BigDecimal;


public class BigDecTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BigDecimal big1=new BigDecimal(1.611);
  	  BigDecimal big2=new BigDecimal(1.6112);
  	  if(big1.compareTo(big2)>0){
  		  System.out.println("big1>big2");
  	  }
  	  else if(big1.compareTo(big2)==0){
  		System.out.println("big1=big2");
	  }
  	  else {
    	System.out.println("big1<big2");
  	  }
	}

}
