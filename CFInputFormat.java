import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


public class CFInputFormat extends CombineFileInputFormat<MyFileWritable, Text>{
	
	public CFInputFormat()
	{
		super();
		setMaxSplitSize(67108864);
	}
	
	@Override
	 public RecordReader<MyFileWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)throws IOException{
		String mydelimiter=context.getConfiguration().get("mydelimiter value");
		
		byte[] passdelim=null;
		if(mydelimiter!=null)
		{
			passdelim = mydelimiter.getBytes();
		}
		
		MyCFRecordreader myob=new MyCFRecordreader(passdelim);
		
		return new CombineFileRecordReader<MyFileWritable, Text>((CombineFileSplit)split, context, MyCFRecordreader.class);
		  }
	public boolean issplittable(JobContext context,Path file)
	{
		return false;
	}

}
