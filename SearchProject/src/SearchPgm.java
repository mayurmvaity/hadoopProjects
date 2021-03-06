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


public class SearchPgm {

	public static class SMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		private final static IntWritable one = new IntWritable(1); 
		private Text sentence = new Text();
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String mySearchText = context.getConfiguration().get("myText");
				String line = value.toString();
				String newline = line.toLowerCase();
				String newText = mySearchText.toLowerCase();
				if(mySearchText != null)
				{
					if(newline.contains(newText))
					{
						sentence.set(newline);
						context.write(sentence, one);
					}
				}
				
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// reducer class
	
	public static class SReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values, Context context)
		{
			try
			{
				int sum =0;
				for(IntWritable val : values)
				{
					sum += val.get();
				}
				result.set(sum);
				context.write(key, result);
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
		// declaring user defined separator
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		if(args.length > 2)
		{
			conf.set("myText", args[2]);
		}
		else
		{
			System.out.println("number of args should be 3");
			System.exit(0);
		}
		Job job = Job.getInstance(conf, "String search");
		
		job.setJarByClass(SearchPgm.class);
		
		job.setMapperClass(SMapper.class);
		job.setReducerClass(SReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}
	
}
