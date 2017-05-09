package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class CompareFiles {
	
	private float percentSimCompare(Object[] s1, Object[] s2) throws ArithmeticException{
		int maxlen = 0;
		int[] curr = new int[s2.length + 1];
		for(int i = 0; i < s1.length; i++){
			String str1 = (String)s1[i];
			maxlen+=str1.length();
			int[] prev = curr;
			for(int j = 1; j <= s2.length; j++){
				String str2 = (String)s2[j - 1];
				//System.out.println(str2 +" : "+ str1);
				if(str1.equals(str2)){
					curr[j] = prev[j-1] + str2.length();
				}else if(curr[j-1]>=prev[j]){
					curr[j] = curr[j-1];
				}else{
					curr[j] = prev[j];
				}
			}
		}
		if(maxlen==0){
			throw new ArithmeticException("s1 length = 0");
		}
		float res = curr[s2.length]/new Float(maxlen);
		return (res * 100);
	}
	
	
	public float compareFiles(String fp1, String fp2){
		File f1 = new File(fp1);
		File f2 = new File(fp2);
		ArrayList <String> l1 = new ArrayList<String>();
		ArrayList <String> l2 = new ArrayList<String>();
		String delim = "[ ,/.\t;:<>\"]";
		try {
			BufferedReader br = new BufferedReader(new FileReader(f1));
			String sp = br.readLine();
			while(sp!= null){
				String[] arr = sp.split(delim);
				for(int i = 0; i < arr.length; i++){
					String s = arr[i].trim();
					if(!s.equals("")){
						l1.add(s);
					}
				}
				sp = br.readLine();
			}
			
			br.close();
			br = new BufferedReader(new FileReader(f2));
			sp = br.readLine();
			while(sp!= null){
				String[] arr = sp.split(delim);
				for(int i = 0; i < arr.length; i++){
					String s = arr[i].trim();
					if(!s.equals("")){
						l2.add(s);
					}
				}
				sp = br.readLine();
			}
			br.close();
			float common = percentSimCompare(l1.toArray(), l2.toArray());
			//System.out.println(common);
			return common;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}catch(ArithmeticException e){
			e.printStackTrace();
			return -1;
		}
		
	}
}
