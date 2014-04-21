
import java.io.*;

import org.apache.hadoop.mapred.*;

public class HDFSGetSplitInfo {


	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			System.out
					.println("Usage: HDFSGetSplitInfo [HDFS file path]");
			System.out
			        .println("HDFS file path like: hdfs://127.0.0.1:54310/user/hduser/test");
			return;
		} 
		

		PrintStream std = System.out;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		System.setOut(new PrintStream(baos));
		
		String filename = args[0];
		
		JobConf conf = new JobConf();

	    FileInputFormat.setInputPaths(conf, filename);
	    TextInputFormat format = new TextInputFormat();
	    format.configure(conf);
	    InputSplit[] splits = format.getSplits(conf, 0);


		System.setOut(std);
	    for(int i=0;i<splits.length;i++){
	    	String[] Locations = splits[i].getLocations();
	    	for(int j=0;j<Locations.length;j++){
	    	    if(j>0)
	    	    	System.out.print(" ");
	    	    System.out.print(Locations[j]);
	    	} 
	    	System.out.println();
	    }
	}
}
