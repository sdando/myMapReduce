package dProxy;

import java.lang.reflect.Proxy;


public class DProxyMain {

	public static IQueryStatus create(QueryStatusImpl qs){
		Class<?>[] interfaces=new Class[]{IQueryStatus.class};
		QueryInvocationHandler handler=new QueryInvocationHandler(qs);
		Object result=Proxy.newProxyInstance(qs.getClass().getClassLoader(),
				interfaces, handler);
		return (IQueryStatus)result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IQueryStatus query=DProxyMain.create(new QueryStatusImpl());
		String status=query.getStatus("memory");
		System.out.println(status);
	}

}
