

class AC{
	public void print(){
		System.out.println("parent method!");
	}
}


public class AsSubClassTest extends AC{

	public void print(){
		System.out.println("child method!");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AC ac=new AC();
		AC ac2=new AsSubClassTest();
		Class aClass=ac2.getClass().asSubclass(ac.getClass());
		ac.print();
		ac2.print();
		System.out.println(aClass.getName());
	}

}
