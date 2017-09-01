import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EmpEx {

	public static class EmpM extends Mapper<LongWritable,Text,Text,Text>
	{
		private Map<String,String> abMap = new HashMap<String,String>();
		private Map<String,String> abMap1 = new HashMap<String,String>();
		
		// private Text opkey = new Text();
		// private Text opvalue = new Text();
		
		// setup method
		public void setup(Context context)
		{
			try
			{
				super.setup(context);
				
				URI files[] = context.getCacheFiles();
				
				Path p = new Path(files[0]);
				Path p1 = new Path(files[1]);
				
				if(p.getName().equals("salary.csv"))
				{
					BufferedReader rd = new BufferedReader(new FileReader(p.toString()));
					String line = rd.readLine();
					while(line!=null)
					{
						String str[] = line.split(",");
						String empid = str[0];
						String empsal = str[1];
						abMap.put(empid, empsal);
						line = rd.readLine();
					}
					rd.close();
					
				}
				
				if(p1.getName().equals("desig.csv"))
				{
					BufferedReader br = new BufferedReader(new FileReader(p1.toString()));
					String line = br.readLine();
					while(line!=null)
					{
						String str[] = line.split(",");
						String eid = str[0];
						String dsgn = str[1];
						abMap1.put(eid, dsgn);
						line = br.readLine();
					}
					br.close();
				}
				if(abMap.isEmpty())
				{
					throw new IOException("Unable to load sal data");
				}
				if(abMap1.isEmpty())
				{
					throw new IOException("Unable to load designation data");
				}
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		
		// map method
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
				String []str = value.toString().split(",");
				String eid = str[0];
				String sal = abMap.get(eid);
				String dsgn = abMap1.get(eid);
				
				String sd = sal+","+dsgn;
				
				context.write(value, new Text(sd));
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
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Reduce side join:: emp example");
		
		job.setJarByClass(EmpEx.class);
		
		job.setMapperClass(EmpM.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		job.addCacheFile(new Path("salary.csv").toUri());
		job.addCacheFile(new Path("desig.csv").toUri());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	
}
