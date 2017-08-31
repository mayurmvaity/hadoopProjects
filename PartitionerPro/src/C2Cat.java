import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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


public class C2Cat extends Configured implements Tool {

	
	// mapper class
	public static class C2CatM extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split(";");
				
				String catid = str[4];
				
				context.write(new Text(catid), value);
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// partitioner class
	public static class C2CatP extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int numReducetasks)
		{
			String str[] = value.toString().split(";");
			char age = str[2].charAt(0);
			
			if(age == 'A') return 0;
			else if(age == 'B') return 1;
			else if(age == 'C') return 2;
			else if(age == 'D') return 3;
			else if(age == 'E') return 4;
			else if(age == 'F') return 5;
			else if(age == 'G') return 6;
			else if(age == 'H') return 7;
			else if(age == 'I') return 8;
			else return 9;
			
		}
	}
	
	// reducer class
	public static class C2CatR extends Reducer<Text,Text,Text,Text>
	{
		public int tcost = 0;
		public int tsales = 0;
		public String age = "";
		public void reduce(Text key,Iterable<Text> values,Context context)
		{
			tcost = 0;
			tsales=0;
			try
			{
				for(Text val: values)
				{
					String str[] = val.toString().split(";");
					int cost = Integer.parseInt(str[7]);
					int sales = Integer.parseInt(str[8]);
					
					tcost +=cost;
					tsales +=sales;
					age = str[2];
					
				}
				if((tsales-tcost)<0)
				{
					context.write(key, new Text(age+"\t"+(tsales-tcost)));
				}
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	// run method
	public int run(String arg[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "C2 loss: category separated by age");
		
		job.setJarByClass(C2Cat.class);
		
		job.setMapperClass(C2CatM.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(C2CatR.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(C2CatP.class);
		job.setNumReduceTasks(10);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		return 0;
	}
	
	// main method
	public static void main(String ar[]) throws Exception
	{
		ToolRunner.run(new Configuration(), new C2Cat(), ar);
		System.exit(0);
	}
	
}
