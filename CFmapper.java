import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CFmapper extends Mapper<MyFileWritable,Text,Text,IntWritable>{
	public void map(MyFileWritable key,Text value,Context context ) throws IOException,InterruptedException
	{
		String s=value.toString();
		String s1=s.replace(","," ");
		for(String myval:s1.split(" "))
		{
			if(myval.length() > 0)
			{
			context.write(new Text(myval),new IntWritable(1));
			}
		}
	}
}
