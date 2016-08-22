import java.io.*;
import java.lang.*;

public class compute {
    public static void main(String[] args) {
	File folder = new File("/home/lolster/Desktop/traffic");
	File[] listOfFiles = folder.listFiles();
	String greatFile = "";
	double greatAvg = 0;
	for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String csvFile = "traffic/"+listOfFiles[i].getName();
				String line = "";
				String cvsSplitBy = ",";
				double avg = 0;
				try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
					line = br.readLine();
					while ((line = br.readLine()) != null) {
						// use comma as separator
						String[] cols = line.split(cvsSplitBy);
						//Legal count
						if(cols[5].matches("2014-08-..T08:..:..")){		
							//System.out.println("True"+cols[5] + "|" + cols[6]);
							avg += Double.parseDouble(cols[6]);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				avg = avg/31;
				avg =  Math.round(avg * 100.0) / 100.0;
				//if(avg > greatAvg){
				//	greatAvg = avg;
				//	greatFile = listOfFiles[i].getName();		
				//}
				System.out.println(listOfFiles[i].getName() +","+avg);
			}
		}
		//System.out.println(greatFile+" | "+greatAvg);
    }
}
