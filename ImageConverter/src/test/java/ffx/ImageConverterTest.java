package ffx;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class ImageConverterTest {
	
	MapDriver<IntWritable, Text, Text, Text> mapDriver;

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
