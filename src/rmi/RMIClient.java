package rmi;

import java.rmi.Naming;



public class RMIClient {

    public static final String RMI_URL="rmi://Namenode/printMes";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			RMIInterface rmiObject=(RMIInterface)Naming.lookup(RMI_URL);
			System.out.println(rmiObject.printMes());		
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
