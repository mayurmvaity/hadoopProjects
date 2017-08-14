
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HighTranxPgm {

	// Mapper class
	
	public static class HTMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String[] str = value.toString().split(";");
				String mykey = "all";
				String dt = str[0];
				String custid = str[1].trim();
				String sales = str[8];
				String myValue = dt + ',' + custid + ',' + sales;
				context.write(new Text(mykey), new Text(myValue));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// Reducer class
	
	public static class HTReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		long maxvalue = 0;
		String custid = "";
		String dt = "";
		
		
		for(Text val : values)
		{
			String[] token = val.toString().split(",");
			if(Long.parseLong(token[2]) > maxvalue)
			{
				maxvalue = Long.parseLong(token[2]);
				dt = token[0];
				custid = token[1];
			}
		}
		String myMaxValue = String.format("%d", maxvalue);
		String myValue = dt+','+custid+','+myMaxValue;
		
		context.write(NullWritable.get(), new Text(myValue));
		}
	}
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Highest value single tranx");
		job.setJarByClass(HighTranxPgm.class);
		job.setMapperClass(HTMapper.class);
		job.setReducerClass(HTReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
