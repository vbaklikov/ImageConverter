package ffx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImageDownloaderRecordReader extends
		RecordReader<IntWritable, Text> {

	private boolean singletonEmit;
	private String urls;
	private long start_line;
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		//emits one record with key=starting-linenumber and value=list-of-folders
		FileSplit f = (FileSplit) split;
		Path path = f.getPath();
		Configuration conf = context.getConfiguration();
		FileSystem fs = path.getFileSystem(conf);
		
		//get the key
		start_line = f.getStart();
		//get the number of lines to process
		long num_lines = f.getLength();

		singletonEmit = false;

		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		int i = 0;
		while(i < start_line && reader.readLine() != null){
			i++;
		}

		urls = "";
		String line;
		for(i = 0; i < num_lines && (line = reader.readLine()) != null; i++){
			urls += line + '\n';
		}
		reader.close();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(singletonEmit == false){
			singletonEmit = true;
			return true;
		}
		else
			return false;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		
		return new IntWritable((int)start_line);
	}



	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return new Text(urls);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (singletonEmit) {
			return 1.0f;
			} else {
			return 0.0f;
			}
	}

	@Override
	public void close() throws IOException {
		return;

	}

}
