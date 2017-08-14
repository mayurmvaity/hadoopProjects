
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GrossPProdPgm {

	// for product 
	// Mapper class
	
	public static class GPMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String[] str = value.toString().split(";");
				String prodid = str[5].trim();
				String sales = str[8];
				String cost = str[7];
				String myValue = cost+','+sales;
				// Long profit = Long.parseLong(str[8]) - Long.parseLong(str[7]);
			
				context.write(new Text(prodid), new Text(myValue));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// Reducer class
	
	public static class GPReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		
		Double pPercent = 0.0d;
		
		
		Double totalCost=0.0;
		Double totalSales = 0.0;
		
		for(Text val : values)
		{
			String[] str = val.toString().split(",");
			Long cost = Long.parseLong(str[0]);
			totalCost += cost;
			
			Long sales = Long.parseLong(str[1]);
			totalSales += sales;
			
		}
		
		Double t = (totalSales - totalCost)/(totalCost)*100;
		
		context.write(new Text(key), new DoubleWritable(t));
		}
	}
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Propercentage by product id");
		job.setJarByClass(GrossPProdPgm.class);
		job.setMapperClass(GPMapper.class);
		job.setReducerClass(GPReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
