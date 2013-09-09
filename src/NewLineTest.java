import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;


public class NewLineTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		FileOutputStream outFile=new FileOutputStream("ttt.txt");
        BufferedWriter out=new BufferedWriter(new OutputStreamWriter(outFile));
        String[] ssStrings={"adsfs","adf"};
        out.write(ssStrings[0]);
        out.newLine();
        out.write(ssStrings[1]);
        out.close();
	}

}
