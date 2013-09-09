package log;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Log4j {
	private static Logger log=Logger.getLogger(Log4j.class);
	public static void main(String[] arsgs){
		PropertyConfigurator.configure("src/log4j.properties");
		log.trace("trace out");
		log.debug("debug out");
		try{
			String s=null;
			int length=s.length();			
		}
		catch (Exception e) {
			// TODO: handle exception
			log.info("s is null");
			log.warn("error");
			log.error("stop");
		}
	}
}
