/**
 * 
 */
package ffx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author vb
 *
 */
public class ImageDownloader extends Configured implements Tool {

	
	@Override
	public int run(String[] args) throws Exception {
		
		// Read in the configuration file
		if (args.length < 3){
			System.out.println("Usage: downloader <input file> <output file> <nodes>");
			System.exit(0);
		}

		// Setup configuration
		Configuration conf = new Configuration();

		String inputFile = args[0];
		String outputFile = args[1];
		int nodes = Integer.parseInt(args[2]);

		String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/')+1);
		System.out.println("Output HIB: " + outputPath);


		conf.setInt("downloader.nodes", nodes);
		conf.setStrings("downloader.outfile", outputFile);
		conf.setStrings("downloader.outpath", outputPath);

		Job job = new Job(conf, "downloader");
		job.setJarByClass(ImageDownloader.class);
		job.setMapperClass(ImageDownloaderMapper.class);
		job.setReducerClass(ImageDownloaderReducer.class);

	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ImageDownloaderInputFormat.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFile + "_output"));

		ImageDownloaderInputFormat.setInputPaths(job, new Path(inputFile));

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ImageDownloader(), args);
		System.exit(res);
	}

}
