package dProxy;


public class QueryStatusImpl implements IQueryStatus {

	private final static String status="running";
	@Override
	public String getStatus(String name) {
		// TODO Auto-generated method stub
		return status;
	}

}
