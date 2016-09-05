import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class main {
	public static void main(String[] args) throws Exception {
		System.out.println("Hello in main()");
		
		String inputFile = args[0]; //file with adjacency matrix
		String inputVector = args[1]; //file with the input vector
		String outputDir = args[2]; //Starting output dir

		Configuration conf = new Configuration();
		
		Path outputPath = new Path(outputDir);
		//Deleting the output dir
		outputPath.getFileSystem(conf).delete(outputPath, true);
		//Creating the output dir
		outputPath.getFileSystem(conf).mkdirs(outputPath);

		int i = 1;
		Path inputPath = new Path(outputPath, "input.txt");
		while(i < 6) {
			Path jobOutputPath = new Path(outputPath, Integer.toString(i));
			calcPageRank(inputPath, jobOutputPath);
			inputPath = jobOutputPath;
			i++;
		}
	}

	//Method which inititate matrix-vector multiplication
	public static void calcPageRank(Path inputPath, Path outputPath) throws Exception {
		System.out.println("Hello in calcPageRank()");
		
		Configuration conf = new Configuration();
		conf.set("row1", "4"); // set the matrix1 dimension here.
		conf.set("col1", "4"); // set the matrix1 dimension here.
		conf.set("row2", "4"); // set the matrix2 dimension here.
		conf.set("col2", "1"); // set the matrix2 dimension here.
		
		Job job = Job.getInstance(conf, "pagerank-multiplication");//new Job(conf, "pagerankjob");
		
		job.setJarByClass(main.class);
		job.setMapperClass(matrix_map.class);
		job.setReducerClass(matrix_reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if(!job.waitForCompletion(true)) {
			throw new Exception("Job failed!");
		}
	}
}
