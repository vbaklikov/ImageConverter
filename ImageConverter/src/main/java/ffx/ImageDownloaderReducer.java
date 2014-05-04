package ffx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.HipiImageBundle;

public class ImageDownloaderReducer extends
		Reducer<BooleanWritable, Text, BooleanWritable, Text> {

	private static Configuration config;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		config = context.getConfiguration();
		
	}
	
	protected void reduce(BooleanWritable key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		if(key.get()){
			FileSystem fileSystem = FileSystem.get(config);
			HipiImageBundle hib = new HipiImageBundle(new Path(config.get("downloader.outfile")), config);
			hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
			for (Text temp_string : values) {
				Path temp_path = new Path(temp_string.toString());
				HipiImageBundle input_bundle = new HipiImageBundle(temp_path, config);
				hib.append(input_bundle);
	
				Path index_path = input_bundle.getPath();
				Path data_path = new Path(index_path.toString() + ".dat");
				System.out.println("Deleting: " + data_path.toString());
				fileSystem.delete(index_path, false);
				fileSystem.delete(data_path, false);
	
				context.write(new BooleanWritable(true), new Text(input_bundle.getPath().toString()));
				context.progress();
			}
			hib.close();
		}
	}

	

}
