package dProxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;


public class QueryInvocationHandler implements InvocationHandler {

	private QueryStatusImpl qs;
	public QueryInvocationHandler(QueryStatusImpl qs){
		this.qs=qs;	
	}
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		// TODO Auto-generated method stub
		Object ret=null;
		String msg=MessageFormat.format("Calling method {0}({1})", method.getName(),
				Arrays.toString(args));
		System.out.println(msg);
		ret=method.invoke(qs, args);
		return ret;
	}

}
