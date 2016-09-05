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


public class matrix_reduce extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
						throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();

		//Row Matrix1
		String row1 = conf.get("row1");
		int r1 = Integer.parseInt(row1);

		//Column Matrix2
		String col2 = conf.get("col2");
		int c2 = Integer.parseInt(col2);
		
		int dim = (r1 * c2)+1;
		double[] row = new double[dim]; 
		double[] col = new double[dim];
		
		for(Text val : values) {
			
			String[] entries = val.toString().split(",");
			if(entries[0].matches("a")) {
				int index = Integer.parseInt(entries[2].trim());
				row[index] = Double.parseDouble(entries[3].trim());
			}
			if(entries[0].matches("b")) {
				int index = Integer.parseInt(entries[1].trim());
				col[index] = Double.parseDouble(entries[3].trim());
			}
		}
		
		//Matrix Multiplication
		double total = 0;
		for(int i = 0 ; i < 5; i++) {
			total += row[i]*col[i];
		}
		context.write(key, new DoubleWritable(total));
	}
}
