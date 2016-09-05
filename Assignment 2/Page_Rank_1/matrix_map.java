/*Authors
Anush S. Kumar (01FB14ECS037)
Sushrith Arkal (01FB14ECS262)
*/

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


	public class matrix_map extends Mapper<LongWritable,Text,Text,Text> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
						throws IOException, InterruptedException{
		//CSV line split 
		String line = value.toString();
		String[] entry = line.split(",");
		String sKey = "";
		String mat = entry[0].trim();
		
		String row, col;
		
		Configuration conf = context.getConfiguration();
		//Row Matrix1
		String row1 = conf.get("row1");
		int r1 = Integer.parseInt(row1);

		//Column Matrix2
		String col2 = conf.get("col2");
		int c2 = Integer.parseInt(col2);
		
		if(mat.matches("a")){
			for (int i =0; i < c2 ; i++) {
				row = entry[1].trim(); // rowid
				sKey = row+i;
				context.write(new Text(sKey),value);
			}
		}
		
		if(mat.matches("b")){
			for (int i =0; i < r1 ; i++){
				col = entry[2].trim(); // colid
				sKey = i+col;
				context.write(new Text(sKey),value);
			}
		}	
	}
}

