import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class POSpart extends Configured implements Tool {

	public static class MClass extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split(",");
				String prodid = str[1];
				
				String val = str[0]+','+str[2];
				context.write(new Text(prodid), new Text(val));
				
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	//reducer class
	
	public static class RClass extends Reducer<Text, Text, Text, IntWritable>
	{
		public int total =0;
		
		
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			total = 0;
			String myValue = "";
			try
			{
				for(Text val : values)
				{
					
					String []str = val.toString().split(",");
					total += Integer.parseInt(str[1]);
					
					
					
				}
				context.write(key, new IntWritable(total));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
		}
	}
	
	// partitioner classs
	public static class PClass extends Partitioner<Text, Text>
	{
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			String str[] = value.toString().split(",");
			if(Integer.parseInt(str[0]) == 21)
			{
				return 0;
			}
			else
			{
				return 1;
			}
		}
	}
	
	// run method
	public int run(String[] arg) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(POSpart.class);
		
		job.setJobName("Total qty sold");
		
		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		job.setMapperClass(MClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(PClass.class);
		
		job.setReducerClass(RClass.class);
		job.setNumReduceTasks(2);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		return 0;
	}
	
	// main method
	
	public static void main(String ar[]) throws Exception
	{
		ToolRunner.run(new Configuration(), new POSpart(), ar);
		System.exit(0);
	}
	
}
