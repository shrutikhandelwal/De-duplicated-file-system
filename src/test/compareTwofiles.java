package test;

public class compareTwofiles {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String file1 = args[0]/*"C:/test/html001.html"*/;
		String file2 = args[1]/*"C:/test/html003.html"*/;
		CompareFiles c = new CompareFiles();
		System.out.println(c.compareFiles(file1, file2));
	}

}
