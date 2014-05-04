package ffx;

import java.io.FileInputStream;
import java.util.List;

public class ModcaFile {
	
	private FileInputStream _fs;
	private List<String> _metaData;
	
	public ModcaFile(String path){
		//perform the conversion 
	}

	public FileInputStream get_fs() {
		return _fs;
	}


	public List<String> get_metaData() {
		return _metaData;
	}


}
