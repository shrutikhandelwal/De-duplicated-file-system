

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class Rabin {

	public static void main(String args[]) {
		Path p1 = FileSystems.getDefault().getPath("/", args[0]);
		Path p2 = FileSystems.getDefault().getPath("/", args[0]);
		ContentBasedChunking cbc = new ContentBasedChunking();
		
		try {
			cbc.digest(Files.readAllBytes(p1));
			System.out.println("new one");
			cbc.digest(Files.readAllBytes(p2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
