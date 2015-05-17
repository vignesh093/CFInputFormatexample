import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


public class MyCFRecordreader extends RecordReader<MyFileWritable,Text>{
	private MyFileWritable key;
	public Text value;
	public FileSystem  fs;
	public Path mypath;
	long pos,start,end;
	public CFLinereader myline;
	byte[] delimiterr;
	public  MyCFRecordreader(byte[] delimiterbytes)
	{		
		delimiterr=delimiterbytes;
	}
	public  MyCFRecordreader(CombineFileSplit mysplit,TaskAttemptContext context,Integer cloff) throws IOException
	{
		//CombineFileSplit mysplit=split;
		mypath=mysplit.getPath(cloff);
		System.out.println("mypath is" +mypath.toString());
		fs=mypath.getFileSystem(context.getConfiguration());
		FSDataInputStream in=fs.open(mypath);
		myline=new CFLinereader(in,delimiterr);
		start=mysplit.getOffset(cloff);
		System.out.println("mypath is" +start);
		end=start+mysplit.getLength(cloff);
		System.out.println("mypath is" +end);
		pos=start;
	}
	
	public MyCFRecordreader() {
		
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key==null)
		{
			System.out.println("mypath is" +(mypath.getName()).toString());
			key=new MyFileWritable();
			key.filename=new Text(mypath.getName());
		}
		key.myoffset=new LongWritable(pos);
		if(value==null)
		{
			value=new Text();
		}
		int newSize=0;
		if(start<end)
		{
			newSize=myline.readLine(value);
			pos+=newSize;
		}
		if(newSize==0)
		{
			key=null;
			value=null;
			return false;
		}
		else
		return  true;
	}
	
	@Override
	public MyFileWritable getCurrentKey() throws IOException,
			InterruptedException {
		
		return key;
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		 if (start == end) {
			    return 0;
			  }
			  return Math.min(1.0f, (pos - start) / (float) (end - start));
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// do nothing
		
	}
	
	@Override
	public void close() throws IOException {
		// do nothing
		
	}
	
}
