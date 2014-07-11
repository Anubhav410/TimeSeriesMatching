package dtw_1;
import java.io.IOException ;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	static final long MEGABYTES = 1024*1024;
	static final int m = 2;	//hardcoding the length of query.txt

	private static List<String> parseArguments(String args[], Job j) {		

		List<String> argList = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-r".equals(args[i])) {
					// set the number of reducers to the specified parameter
					j.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					argList.add(args[i]);				
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
			}
		}

		return argList;
	}


	public static int main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();


		// Add resources
		conf.addResource("hdfs-default.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-default.xml");
		conf.addResource("mapred-site.xml");	

		//		System.out.println("here1");

		Job job = new Job(conf);
		job.setJobName("WordCount");

		//		System.out.println("here2");

		List<String> other_args = parseArguments(args, job);	

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);


		//		System.out.println("here3");

		// the keys are words (strings)
		job.setOutputKeyClass(DoubleWritable.class);
		// the values are counts (ints)
		job.setOutputValueClass(DoubleWritable.class);		

		//		System.out.println("here4");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//Set the input format class
		job.setInputFormatClass(TextInputFormat.class);

		//Set the output format class
		//		System.out.println("here6");

		//Set the input path
		TextInputFormat.setInputPaths(job,other_args.get(0));
		
		
		
		
		//Set the output path
		TextOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
//		TextInputFormat.setMinInputSplitSize(job,2 * Double.SIZE);
//		TextInputFormat.setMaxInputSplitSize(job,2 * Double.SIZE);				

		// Set the jar file to run
		job.setJarByClass(Driver.class);

		// Submit the job
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);	

		int exitCode = job.waitForCompletion(true) ? 0 : 1;


		if( exitCode == 0) {			
			Date end_time = new Date();
			System.out.println("Job ended: " + end_time);
			System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");						
		} else {
			System.out.println("Job Failed!!!");
		}

		return exitCode;

	}

}
