package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;





public class crawler {

	private String HTMLPath = "C:\\Test";
	private int maxpages = 0;
	private Queue<String> crawlerQueue; 
	private String sourceLink;
	private int ctr = 0;
	public crawler(String link, String path, int maxpages){
		HTMLPath = path;
		sourceLink = link;
		this.maxpages = maxpages;
		crawlerQueue = new LinkedList<String>();
		crawlerQueue.add(link);
	}
	
	public void startCrawling(){
		while(ctr<maxpages && !crawlerQueue.isEmpty()){
			crawl((String)crawlerQueue.remove());
		}
	}
	
	void crawl(String surl){
		System.out.println("Crawling for: " + surl);

		File dir = new File(HTMLPath);
		if(!dir.exists() || !dir.isDirectory()){
			dir.mkdirs();
		}
		
		String filename = String.format("%03d", ++ctr) + ".html";
		//System.out.println(filename);
		File file = new File(HTMLPath + filename);

		try {

			URL url = new URL(surl);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			int conCode = con.getResponseCode();

			if (conCode == 200) {
				FileWriter fw = new FileWriter(file);
				BufferedWriter bw = new BufferedWriter(fw);
				InputStream is = con.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String s;
				while ((s = br.readLine()) != null) {
					s = s + "\n";
					bw.write(s);
				}

				bw.close();
				br.close();
				is.close();
				ArrayList<String> links = getLinks(HTMLPath + filename);
				crawlerQueue.addAll(links);
				Cleaner.saveCleanText(file.getAbsolutePath());
			} 

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	ArrayList<String> getLinks(String filename){
		ArrayList<String> urls = new ArrayList<String>();
		
		File file = new File(filename);
		try {
			Document doc = Jsoup.parse(file, "UTF-8");
			Elements aLinks = doc.select("a[href]");
			for (Element e : aLinks) {
				String annotation = e.outerHtml();
				int beginIndex = annotation.indexOf("href");
				beginIndex = beginIndex + 6;
				int endIndex = annotation.indexOf("\"", beginIndex);
				String link = annotation.substring(beginIndex, endIndex);
				String title = e.html();
				if ( e.children().size() > 0 ) {
					title = "- NA since it has a nested HTML markup -";
				}

				if (!link.startsWith("http")) {
					if (link.length() > 1) {
						if (!link.startsWith("#"))
							link = sourceLink + link.substring(1);
						else
							link = "";
					} else {
						link = "";
					}
				}
				// System.out.println(link);
				if (!link.equals(""))
					urls.add(link);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return urls;
	}
	
}
