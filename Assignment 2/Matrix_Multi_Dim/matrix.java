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


public class matrix{
	//Function to extract the matrix from the file (Input Format: [matrix name, row, column, value] )
	public static class Matrix_Mapper extends Mapper<LongWritable,Text,Text,Text> {

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

public static class Matrix_Reducer extends Reducer<Text, Text, Text, IntWritable> {
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
		int[] row = new int[dim]; 
		int[] col = new int[dim];
		
		for(Text val : values) {
			
			String[] entries = val.toString().split(",");
			if(entries[0].matches("a")) {
				int index = Integer.parseInt(entries[2].trim());
				row[index] = Integer.parseInt(entries[3].trim());
			}
			if(entries[0].matches("b")) {
				int index = Integer.parseInt(entries[1].trim());
				col[index] = Integer.parseInt(entries[3].trim());
			}
		}
		
		//Matrix Multiplication
		int total = 0;
		for(int i = 0 ; i < 5; i++) {
			total += row[i]*col[i];
		}
		context.write(key, new IntWritable(total));
	}
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		
		if(args.length !=2)
		{
			System.err.println("Usage : Weather <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("row1", "2"); // set the matrix1 dimension here.
		conf.set("col1", "3"); // set the matrix1 dimension here.
		conf.set("row2", "3"); // set the matrix2 dimension here.
		conf.set("col2", "2"); // set the matrix2 dimension here.
		Job job = Job.getInstance(conf);

		FileSystem fs = FileSystem.get(conf);
		job.setJarByClass(matrix.class);
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Matrix_Mapper.class);
		job.setReducerClass(Matrix_Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		if(!fs.exists(input)) {
			System.err.println("Input file doesn't exists");
			return;
		}
		if(fs.exists(output)) {
			fs.delete(output, true);
			System.err.println("Output file deleted");
		}
		fs.close();
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);	
	}
}

