import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GrossPCatPgm {

	// Mapper class
	// For GPP Category
	
	public static class GPCatMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String []str = value.toString().split(";");
				String mykey = str[4].trim();
				String sales = str[8];
				String cost = str[7];
				
				String myValue = cost+','+sales;
				
				context.write(new Text(mykey), new Text(myValue));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
		}
		
	
	}
	
	// Reducer class
	
	public static class GPCatReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Double tcost = 0.0d;
			Double tsales = 0.0d;
			
			for(Text val : values)
			{
				String str[] = val.toString().split(",");
				
				Double cost = Double.parseDouble(str[0]);
				Double sales = Double.parseDouble(str[1]);
				
				tcost += cost;
				tsales += sales;
				
			}
			
			Double t = (tsales - tcost)/(tcost)*100;
			
			context.write(new Text(key), new DoubleWritable(t));
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Gross PP cat");
		
		job.setJarByClass(GrossPCatPgm.class);
		job.setMapperClass(GPCatMapper.class);
		job.setReducerClass(GPCatReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
