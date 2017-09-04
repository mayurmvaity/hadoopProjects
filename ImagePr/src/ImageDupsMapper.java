import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ImageDupsMapper extends Mapper<Text,BytesWritable,Text,Text> {

	public void map(Text key,BytesWritable value,Context context) throws InterruptedException, IOException
	{
		String md5str = new String();
		try
		{
			md5str = calculateMd5(value.getBytes());
			
			
		}
		catch(NoSuchAlgorithmException e)
		{
			e.printStackTrace();
			context.setStatus("Internal error - cannot find calculatemd5 algorithm");
			return;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		context.write(new Text(md5str), key);
	}

	// calculatemd5 method
	
	static String calculateMd5(byte[] imagedata) throws NoSuchAlgorithmException
	{
		// get md5 for given image data
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(imagedata);
		
		byte[] hash = md.digest();
		
		// converting to md5 data
		String hexString = new String();
		for(int i=0;i<hash.length;i++)
		{
			hexString += Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1);
		}
		return hexString;
	}
	
	
}
