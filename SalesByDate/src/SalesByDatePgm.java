import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SalesByDatePgm {

	//Mapper class
	
	public static class DtMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split(";");
				String dt = str[0];
				
				Long sales = Long.parseLong(str[8]);
				
				context.write(new Text(dt), new LongWritable(sales));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		
	}
	
	// Reducer class
	
	public static class DtReducer extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
		{
			Long sum = 0l;
			try
			{
				for(LongWritable val : values)
				{
					sum += val.get();
				}
				
				context.write(key, new LongWritable(sum));
				
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
		Job job = Job.getInstance(conf, "Sales by date");
		
		job.setJarByClass(SalesByDatePgm.class);
		
		job.setMapperClass(DtMapper.class);
		job.setReducerClass(DtReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
