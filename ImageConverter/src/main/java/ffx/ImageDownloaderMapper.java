package ffx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringReader;
//import java.net.URL;
//import java.net.URLConnection;







import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.io.Files;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.HipiImageBundle;

public class ImageDownloaderMapper extends
		Mapper<IntWritable, Text, BooleanWritable, Text> {

	
	private static Configuration config;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		config = context.getConfiguration();
		
	}
	
	@Override
	protected void map(IntWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		String temp_path = config.get("downloader.outpath");
		System.out.println("Temp path: " + temp_path);

		HipiImageBundle hib;

		String word = value.toString();

		try {
			BufferedReader reader = new BufferedReader(new StringReader(word));
				
			String imagePath;
			
			while((imagePath = reader.readLine()) != null){
				
					File folder = new File(imagePath);
					if (folder.exists()) {

						long startT = 0;
						long stopT = 0;
						startT = System.currentTimeMillis();

						String batchNumber = folder.getName();
						hib = new HipiImageBundle(new Path(temp_path + batchNumber
								+ ".hib"), config);
						hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
						for (File file : folder.listFiles()) {
							if (Files.getFileExtension(file.getName()).compareTo(
									".modca") == 0) {

								//replace with actual implementation of conversion
								FileInputStream fs = new FileInputStream(file);
								hib.addImage(fs, ImageType.JPEG_IMAGE);
							}
						}
						hib.close();
						context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));

						stopT = System.currentTimeMillis();
						float el = (float) (stopT - startT) / 1000.0f;
						System.err.println("> Took " + el + " seconds\n");

					}
				
			}
			reader.close();
			
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error... probably cluster downtime");
		}

	}
		

}
