/*Authors
Anush S. Kumar (01FB14ECS037)
Sushrith Arkal (01FB14ECS262)
*/

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class matrix{
	//Function to extract the matrix from the file (Input Format: [matrix name, row, column, value] )
	public static class Matrix_Mapper extends Mapper<LongWritable,Text,Text,Text> {

		@Override
		protected void map(LongWritable key, Text value,Context context)
							throws IOException, InterruptedException {
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
					sKey = row+","+i+",";
					context.write(new Text(sKey),value);
				}
			}
			
			if(mat.matches("b")){
				for (int i =0; i < r1 ; i++){
					col = entry[2].trim(); // colid
					sKey = i+","+col+",";
					context.write(new Text(sKey),value);
				}
			}	
		}
	}
	//Function to extract the matrix from the file (Input Format: [matrix name, row, column, value] )
	public static class Matrix_Mapper1 extends Mapper<LongWritable,Text,Text,Text> {

		@Override
		protected void map(LongWritable key, Text value,Context context)
							throws IOException, InterruptedException {
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
					sKey = row+","+i+",";
					context.write(new Text(sKey),value);
				}
			}
			
			if(mat.matches("b")){
				for (int i =0; i < r1 ; i++){
					col = entry[2].trim(); // colid
					sKey = i+","+col+",";
					context.write(new Text(sKey),value);
				}
			}	
		}
	}
	//Function to reduce the mapped data from the Map phase
	public static class Matrix_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
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
			String temp = "b,"+key.toString();
			context.write(new Text(temp), new DoubleWritable(total));
		}
	}
	//Function to extract and map vector for comparison in the reduce phase
	public static class Compare_Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      
	      String[] tockRecord = value.toString().split(",");
	      String key_temp = tockRecord[1]+""+tockRecord[2];
	      context.write(new Text(key_temp), new DoubleWritable(Double.parseDouble(tockRecord[3].trim())));
	    }
	}
	//Function to reduce(compare) the mapped data from the Map phase of vectors
	public static class Compare_Reduce extends Reducer<Text,DoubleWritable,Text,Text> {
	    public static final int CONV_SCALE = 1000;
	    public static enum CountersComp {
	        DELTAS
	    }
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	      double val1 = 0;
	      double val2 = 0;
	      Iterator<DoubleWritable> valuesIt = values.iterator();
	      if(valuesIt.hasNext())
	          val1 = valuesIt.next().get();
	      if(valuesIt.hasNext())
	          val2 = valuesIt.next().get();
	      if(valuesIt.hasNext() || (val1 == 0 && val2 == 0)) {
	          System.exit(100);
	      }
	      double diff = (double)Math.abs(val1-val2) * CONV_SCALE;
	      context.getCounter(CountersComp.DELTAS).increment((int)diff);
	    }
	}
	//Main method, controls the execution of the map-reduce operations
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, Exception {
			
			if(args.length !=3)	{
				System.err.println("Usage : Weather <input path1> <input path2> <output path>");
				System.exit(-1);
			}
			
			//Transition matrix input
			Path input = new Path(args[0]);
			//Vector input
			Path input1 = new Path(args[1]);

			//Output Dir
			Path output = new Path(args[2]);
			
			int i = 1;
			
			while(true){
				Path jobOutputPath = new Path(output, Integer.toString(i));
				System.out.println("Input Matrix"+i+": "+input);
				System.out.println("Input Vector"+i+": "+input1);
				pageRank(input, input1, jobOutputPath);
				if(i != 1) {
					double res = pageRankCompare(input1, jobOutputPath, 4);
					if(res < 0.01){
						break;
					}
				}

				if(i > 20){
					break;
				}

				input1 = jobOutputPath;
				i++;	
			}	
	}
	//Method to initiate matrix-vector multiplication using map-reduce 
	public static void pageRank(Path input, Path input1, Path output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("row1", "4"); // set the matrix1 dimension here.
		conf.set("col1", "4"); // set the matrix1 dimension here.
		conf.set("row2", "4"); // set the matrix2 dimension here.
		conf.set("col2", "1"); // set the matrix2 dimension here.
		Job job = Job.getInstance(conf);

		FileSystem fs = FileSystem.get(conf);
		job.setJarByClass(matrix.class);
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
	

		//Setting multiple input paths to mappers
		MultipleInputs.addInputPath(job,input,TextInputFormat.class,Matrix_Mapper.class);

		MultipleInputs.addInputPath(job,input1,TextInputFormat.class,Matrix_Mapper1.class);


		//job.setMapperClass(Matrix_Mapper.class);
		job.setReducerClass(Matrix_Reducer.class);

		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		if(fs.exists(output)) {
			fs.delete(output, true);
			System.err.println("Output file deleted");

		}
		fs.close();

		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);	
	}
	//Method to initiate comparison between old and new vector using mapreduce
	public static double pageRankCompare(Path input, Path input1, int numNodes) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(matrix.class);
        //job.setMapOutputKeyClass(Text.class); 
        //job.setMapOutputValueClass(DoubleWritable.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
    	job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
    

        //Setting multiple input paths to mappers
        MultipleInputs.addInputPath(job,input,TextInputFormat.class,Compare_Map.class);

        MultipleInputs.addInputPath(job,input1,TextInputFormat.class,Compare_Map.class);


        //job.setMapperClass(Matrix_Mapper.class);
        job.setReducerClass(Compare_Reduce.class);

        //job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

    	Path rand_out = new Path("/user/random");
       	if(fs.exists(rand_out)) {
            fs.delete(rand_out, true);
            System.err.println("Output file deleted");

        }
        //fs.close();

        //try not putting a output path
        FileOutputFormat.setOutputPath(job,rand_out );
        job.waitForCompletion(true);
        
        double totalConvergence = (double)job.getCounters().findCounter(Compare_Reduce.CountersComp.DELTAS).getValue();
        double convergence = ((double)totalConvergence / (double)Compare_Reduce.CONV_SCALE) / (double) numNodes;
        

        if(fs.exists(rand_out)) {
            fs.delete(rand_out, true);
            System.err.println("Output file deleted");

        }
        fs.close();

        return convergence;
    }
}

