package test;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;



public class Cleaner {

	
	public Cleaner() {
		// TODO Auto-generated constructor stub
	}

	private static String readFile(String filePath) {
		String content = "";
		String line = "";
		try {
            FileReader fileReader = new FileReader(filePath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                content += line;
            }

            // Always close files.
            bufferedReader.close();

		} catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + filePath + "'");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + filePath + "'");
            // Or we could just do this: 
            // ex.printStackTrace();
        }

		return content;
	}
	
	public static void saveCleanText(String filePath) {
		String content = Cleaner.readFile(filePath);
		String cleanContent = Cleaner.removeNoise(content);
		File htmlFile = new File(filePath);
		String fileName = htmlFile.getName();
		String path = "./repository/text/";
		File repoDir = new File(path);
		if(!repoDir.exists() || !repoDir.isDirectory()) {
			repoDir.mkdirs();
		}
		//System.out.println(path + fileName.replace("html", "txt"));
		File textFile = new File(path + fileName.replace("html", "txt"));
		try {
			FileWriter fw = new FileWriter(textFile);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(cleanContent);
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static String removeNoise(String htmlMarkup) {
		// Remove Noise
		String output = "";
		try {
			output = ArticleExtractor.getInstance().getText(htmlMarkup);
		} catch (BoilerpipeProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output;
	}

}
