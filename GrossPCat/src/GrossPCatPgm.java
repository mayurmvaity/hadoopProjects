


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


public class GrossPCatPgm {

	// for Category 
	// Mapper class
	
	public static class GPMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String[] str = value.toString().split(";");
				String catid = str[4].trim();
				Long profit = Long.parseLong(str[8]) - Long.parseLong(str[7]);
			
				context.write(new Text(catid), new LongWritable(profit));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// Reducer class
	
	public static class GPReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
		
		Long totalSales = 0L;
		
		for(LongWritable val : values)
		{
			totalSales += val.get();
			
		}
		
		context.write(new Text(key), new LongWritable(totalSales));
		}
	}
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Highest value single tranx");
		job.setJarByClass(GrossPCatPgm.class);
		job.setMapperClass(GPMapper.class);
		job.setReducerClass(GPReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
