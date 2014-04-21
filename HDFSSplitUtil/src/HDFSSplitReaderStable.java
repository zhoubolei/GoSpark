
import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HDFSSplitReaderStable {


	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out
					.println("Usage: HDFSSplitReader [HDFS file path] [Split index]");
			System.out
			        .println("HDFS file path like: hdfs://127.0.0.1:54310/user/hduser/test");
			System.out
	                .println("Split index like: 0~(Splitnumber-1)");
			return;
		}
		

		PrintStream std = System.out;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		System.setOut(new PrintStream(baos));
		
		//int nsplit = Integer.parseInt(args[2]);
		int splitind = Integer.parseInt(args[1]);
		String filename = args[0];
		
		JobConf conf = new JobConf();
		FileSystem fs = FileSystem.get(conf);

	    FileInputFormat.setInputPaths(conf, filename);
	    TextInputFormat format = new TextInputFormat();
	    format.configure(conf);
	    InputSplit[] splits = format.getSplits(conf, 0);

	    LongWritable key = new LongWritable();
	    Text value = new Text();
	    RecordReader<LongWritable, Text> recordReader =
	      format.getRecordReader(splits[splitind], conf, Reporter.NULL);
	    

		System.setOut(std);
	    while(recordReader.next(key, value)) {
	      System.out.println(value.toString());
	    }
	}
}
