package rmi;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RMIObject extends UnicastRemoteObject implements RMIInterface {

    public static final String RMI_URL="rmi://Namenode/printMes";
	public RMIObject() throws RemoteException{}
	@Override
	public String printMes() throws RemoteException{
		// TODO Auto-generated method stub
		String mes="welcome you!";
		return mes;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			RMIInterface rmiObject=new RMIObject(); 
			Naming.bind(RMI_URL, rmiObject);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}		
	}
}
