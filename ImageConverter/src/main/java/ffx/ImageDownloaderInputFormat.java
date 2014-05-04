package ffx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImageDownloaderInputFormat extends FileInputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		 
		return new ImageDownloaderRecordReader();
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException
	{
		Configuration conf = job.getConfiguration();
		
		//determine how many nodes are we splitting to
		int nodes = conf.getInt("downloader.nodes", 20);


		ArrayList<String> hosts = new ArrayList<String>(0);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		FileSystem fileSystem = FileSystem.get(conf);
		
		//since there is no data in HDFS, create temp files in HDFS to span mappers
		String tempOutputPath = conf.get("downloader.outpath") + "_tmp";
		Path tempOutputDir = new Path(tempOutputPath);

		if (fileSystem.exists(tempOutputDir)){
			fileSystem.delete(tempOutputDir, true);
		}
		fileSystem.mkdirs(tempOutputDir);

		int i = 0;
		while( hosts.size() < nodes && i < 2*nodes)
		{
			String tempFileString = tempOutputPath + "/" + i;
			Path tempFile = new Path(tempFileString);
			FSDataOutputStream os = fileSystem.create(tempFile);
			os.write(i);
			os.close();
	
			//determine the host in HDFS that got the file and ensure ALL nodes are utilized
			//that is if file lands on the "known" host, re-save it elsewhere
			FileStatus match = fileSystem.getFileStatus(tempFile);
			long length = match.getLen();
			BlockLocation[] blocks = fileSystem.getFileBlockLocations(match, 0, length);
	
			boolean save = true;
			for (int j = 0; j < hosts.size(); j++){
				if (blocks[0].getHosts()[0].compareTo(hosts.get(j)) == 0){
					save = false;
					System.out.println("Repeated host: " + i);
					break;
				}
			}
	
			if (save){
				//this is a new unknown host, so add it to the list for utilization
				hosts.add(blocks[0].getHosts()[0]);
				System.out.println("Found host successfully: " + i);
			}
			i++;
		}
	
		System.out.println("Tried to get " + nodes + " nodes, got " + hosts.size());
	
	
		FileStatus file = listStatus(job).get(0);
		Path path = file.getPath();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		int num_lines = 0;
		while(reader.readLine() != null){
			num_lines++;
		}
		reader.close();
	
		//now generate a FileSplit for each host
		//in this case FileSplit is a portion of folders for a given map task
		int span = (int) Math.ceil(((float) (num_lines)) / ((float) hosts.size()));
		int last = num_lines - span * (hosts.size()-1);
		System.out.println("First n-1 nodes responsible for " + span + " images");
		System.out.println("Last node responsible for " + last + " images");
	
		FileSplit[] f = new FileSplit[hosts.size()];
		for (int j = 0; j < f.length; j++){
			String[] host = new String[1];
			host[0] = hosts.get(j);
			if (j < f.length - 1){
				splits.add( new FileSplit(path , (j*span) , span, host));
			} 
			else{
				splits.add( new FileSplit(path , (j*span) , last, host));
			}
		}
	
		if (fileSystem.exists(tempOutputDir)){
			fileSystem.delete(tempOutputDir, true);
		}
		return splits;
	}

}
