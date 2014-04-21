
import java.util.List;
import java.io.EOFException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

public class HDFSSplitReader {
	public static void main(String[] args) {
		/*if (args.length < 2) {
			System.out
					.println("Usage: HDFSSplitReader [HDFS file path] [Split index]");
			return;
		}*/
		//FileInputFormat fif;

		// r = TextInputFormat.getRecordReader();
		// Create a new JobConf
		
		Job job;
		try {
			job = Job.getInstance(new Configuration(), "spark");
			FileInputFormat.setInputPaths(job, new Path("hdfs://127.0.0.1:54310/user/hduser/test"));
			job.setInputFormatClass(TextInputFormat.class);
			//TextInputFormat.setInputPaths(job, new Path("/home/drc/software/util/findclass.sh"));
			

			TextInputFormat ti = new TextInputFormat();
			java.util.List<InputSplit> splits = ti.getSplits(job);

			System.out.print(splits.size());
			long len = splits.get(0).getLength();
			String[] s = splits.get(0).getLocations();

			System.out.print(len);
			System.out.print(s[0]);
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		    String jtid = formatter.format(new Date());
			JobID jid = new JobID(jtid, 0);
			TaskID tid = new TaskID(jid, TaskType.MAP, 0);
			TaskAttemptID taskAttemptID= new TaskAttemptID(tid, 0);
			
			RecordReader<LongWritable, Text> r = ti.createRecordReader(splits.get(0), new TaskAttemptContextImpl(job.getConfiguration(), taskAttemptID));
			for (int i=0;i<10;i++){
				if (r.getCurrentKey() == null){
					System.out.print(i);
				}
				//System.out.print(i);
				
				r.nextKeyValue();
				long k = r.getCurrentKey().get();
				byte[] v = r.getCurrentValue().getBytes();
				System.out.print(k);
				System.out.print(v);
			}
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Specify various job-specific parameters

		/*
		String[] hosts = new String[1];
		hosts[0] = "127.0.0.1";
		FileSplit split = TextInputFormat.makeSplit(new Path(
				"/user/hduser/gutenberg/pg20417.txt"),
                0,
                1000,
                hosts);*/
		//sp = FileInputFormat.getSplits(conf, 1);
                
        //LineRecordReader lr = new LineRecordReader(conf, splits[0]);
		
		//TextInputFormat.getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) 
		//System.out.println(sp.length);
	}
}