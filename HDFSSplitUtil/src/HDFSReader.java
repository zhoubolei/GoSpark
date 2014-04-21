
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class HDFSReader {
	public static void main(String[] args) throws Exception {
		
		PrintStream console = System.err;

		File file = new File("err.txt");
		FileOutputStream fos = new FileOutputStream(file);
		PrintStream ps = new PrintStream(fos);
		System.setErr(ps);
		
		String uri = "hdfs://127.0.0.1:54310/user/hduser/test";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			in.seek(0); // go back to the start of the file
			while (true){
			    IOUtils.copyBytes(in, System.out, 4096, false);
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
