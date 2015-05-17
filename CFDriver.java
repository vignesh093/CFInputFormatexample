
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CFDriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=1)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		
		Configuration conf=new Configuration();
		conf.set("mydelimiter value",".");
		Job job=new Job(conf,"CFDriver");
		job.setJarByClass(CFDriver.class);
		job.setJobName("CFDriver");
		
		FileInputFormat.addInputPaths(job,"/user/hduser/test/cftest1.dat,/user/hduser/test/cftest2.dat,/user/hduser/test/cftest3.dat");
		FileOutputFormat.setOutputPath(job,new Path(args[0]));

		job.setMapperClass(CFmapper.class);
		
		job.setReducerClass(CFReducer.class);
		job.setInputFormatClass(CFInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
