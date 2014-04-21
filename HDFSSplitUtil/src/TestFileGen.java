
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class TestFileGen {
	public static void main(String [] args) throws FileNotFoundException, UnsupportedEncodingException{
		PrintWriter writer = new PrintWriter("testSplitRead.txt");
		for(int i=0;i<10000000;i++) {
		   writer.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		}
		writer.close();
		
	}
}
