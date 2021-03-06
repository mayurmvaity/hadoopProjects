import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgByCity {

	// avg marks by city with and without combiner class
	// mapper class
	
	public static class AMapper extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split(",");
				
				String mykey = str[3];
				Integer score = Integer.parseInt(str[2]);
				
				context.write(new Text(mykey), new FloatWritable(score));
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// reducer class
	public static class AReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
		{
			
			Float sum = 0.0f;
			int count = 0;
			try
			{
				for(FloatWritable val : values)
				{
					sum+= val.get();
					count++;
				}
				
				Float avg = (float) (sum/count);
				
				context.write(key, new FloatWritable(avg));
				
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
		Job job = Job.getInstance(conf, "Avg by city");
		
		job.setJarByClass(AvgByCity.class);
		
		job.setMapperClass(AMapper.class);
		job.setCombinerClass(AReducer.class);
		job.setReducerClass(AReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
		
	}
	
}
