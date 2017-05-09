package test;

public class createTests {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String url = /*args[0]*/"https://en.wikipedia.org/wiki/Rabin_fingerprint";
		String rep = /*args[1]*/"C:/test/html";
		crawler c = new crawler(url, rep, 40);
		c.startCrawling();
		
	}

}
