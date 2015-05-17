import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MyFileWritable implements WritableComparable<MyFileWritable>{
	
	public Text filename;
	public LongWritable myoffset;
	public MyFileWritable()
	{
		
	}
	public MyFileWritable(Text filename,LongWritable myoffset)
	{
		this.filename=filename;
		this.myoffset=myoffset;
	}
	public MyFileWritable(String filename,Long myoffset)
	{
		this.filename=new Text(filename);
		this.myoffset=new LongWritable(myoffset);
	}
	public void setfilename(Text filename)
	{
		this.filename=filename;
	}
	public void setoffset(LongWritable myoffset)
	{
		this.myoffset=myoffset;
	}
	public Text getfilename()
	{
		return new Text(this.filename);
	}
	public LongWritable getoffset()
	{
		return this.myoffset;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		filename.readFields(in);
		myoffset.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		filename.write(out);
		myoffset.write(out);
		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((filename == null) ? 0 : filename.hashCode());
		result = prime * result
				+ ((myoffset == null) ? 0 : myoffset.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyFileWritable other = (MyFileWritable) obj;
		if (filename == null) {
			if (other.filename != null)
				return false;
		} else if (!filename.equals(other.filename))
			return false;
		if (myoffset == null) {
			if (other.myoffset != null)
				return false;
		} else if (!myoffset.equals(other.myoffset))
			return false;
		return true;
	}
	@Override
	public int compareTo(MyFileWritable o) 
	{
		
		int cmp=this.filename.compareTo(o.filename);
		if(cmp==0)
		{
			cmp=(int)Math.signum((double)(this.myoffset.get()) - (o.myoffset.get()));
		}
		return cmp;	
	}
}
