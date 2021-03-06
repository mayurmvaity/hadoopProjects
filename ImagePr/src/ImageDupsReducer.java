import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reducer class
public class ImageDupsReducer extends Reducer<Text,Text,Text,NullWritable> {
	
	public void reduce(Text key,Iterable<Text> values,Context context)
	{
		try
		{
			Text imageFilePath = null;
			
			for(Text filePath:values)
			{
				imageFilePath = filePath;
				break;
			}
			
			context.write(new Text(imageFilePath), NullWritable.get());
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

}
