import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class RandomTest {
	public static void main(String[] args) throws IOException{
		BufferedReader in;
		String file="..//cluster.txt";
		String line;
        in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		while((line=in.readLine())!=null)
		{
			String[] schar=line.split("\t");
			for(String ss:schar)
				System.out.println(ss);
			System.out.println();
		}
	}

}
