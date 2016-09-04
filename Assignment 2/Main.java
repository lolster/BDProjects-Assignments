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

public class Main {
	public static void main(String[] args) throws Exception {
		System.out.println("Hello in main()");
		String inputFile = args[0]; //file with adjacency matrix
		String outputDir = args[1];

		Configuration conf = new Configuration();
		Path outputPath = new Path(outputDir);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		outputPath.getFileSystem(conf).mkdirs(outputPath);

		Path inputPath = new Path(outputPath, "input.txt");
		int numNodes = createInputFile(new Path(inputFile), inputPath);

		int i = 1;
		double desiredConvergence = 0.01;

		while(true) {
			Path jobOutputPath = new Path(outputPath, Integer.toString(i));

			if(calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence) {
				break;
			}
			inputPath = jobOutputPath;
			i++;
		}
	}

	public static int createInputFile(Path file, Path targetFile) throws IOException {
		System.out.println("Hello in createInputFile()");
		Configuration conf = new Configuration();

		FileSystem fs = file.getFileSystem(conf);
		
		int numNodes = getNumNodes(file);
		double initialPageRank = 1.00;

		OutputStream os = fs.create(targetFile);
		LineIterator it = IOUtils.lineIterator(fs.open(file), "UTF8");

		while(it.hasNext()) {
			String line = it.nextLine();
			String[] parts = line.split("\t");

			Node node = new Node(initialPageRank, Arrays.copyOfRange(parts, 1, parts.length));
			IOUtils.write(parts[0] + "\t" + node.toString() + "\n", os);
		}
		os.close();
		return numNodes;
	}

	public static int getNumNodes(Path file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);
		LineIterator it = IOUtils.lineIterator(fs.open(file), "UTF8");
		int len = 0;
		while(it.hasNext()) {
			it.nextLine();
			len++;
		}
		return len;
	}

	public static double calcPageRank(Path inputPath, Path outputPath, int numNodes) throws Exception {
		System.out.println("Hello in calcPageRank()");
		Configuration conf = new Configuration();

		conf.setInt(Reduce.CONF_NUM_NODES, numNodes);

		Job job = Job.getInstance(conf, "pagerankjob");//new Job(conf, "pagerankjob");
		job.setJarByClass(Main.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if(!job.waitForCompletion(true)) {
			throw new Exception("Job failed!");
		}

		long totalConvergence = job.getCounters().findCounter(Reduce.Counter.DELTAS).getValue();
		double convergence = ((double)totalConvergence / (double)Reduce.CONVERGENCE_SCALING_FACTOR) / (double) numNodes;

		return convergence;
	}
}
