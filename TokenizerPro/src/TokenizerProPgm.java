import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TokenizerProPgm {

	public static class TMapper extends Mapper<LongWritable, Text, Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				Text word = new Text();
				IntWritable one = new IntWritable(1);
				
				StringTokenizer str = new StringTokenizer(value.toString());
				
				while(str.hasMoreTokens())
				{
					String myWord = str.nextToken().toLowerCase();
					
					word.set(myWord);
					context.write(word, one);
				}
				
				
			}
			catch(Exception r)
			{
				System.out.println(r.getMessage());
			}
		
		}
	}
	
	// reducer class
	public static class TReducer extends Reducer<Text, IntWritable,Text, IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> values, Context context)
		{
			int sum = 0;
			
			try
			{
				for(IntWritable val : values)
				{
					sum += val.get();
				}
				
				context.write(key, new IntWritable(sum));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// main method
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Tokenizer pgm");
		
		job.setJarByClass(TokenizerProPgm.class);
		
		job.setMapperClass(TMapper.class);
		job.setReducerClass(TReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
	
}
