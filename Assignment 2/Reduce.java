import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,Text,Text,Text> {
	public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	public static final String CONF_NUM_NODES = "pagerank.numnodes";
	private int numberOfNodesInGraph;
	
	public static enum Counter {
		DELTAS
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		numberOfNodesInGraph = context.getConfiguration().getInt(CONF_NUM_NODES, 0);
	}

	private Text outValue = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		double summedPageRanks = 0;
		Node originalNode = new Node();
		
		for (Text textValue : values) {
			Node node = Node.parseMR(textValue.toString());

			if (node.containsAdjacentNodes()) {
				originalNode = node;
			} else {
				summedPageRanks += node.getPageRank();
			}
		}
		
		double newPageRank = summedPageRanks;
		
		double delta = originalNode.getPageRank() - newPageRank;
		
		originalNode.setPageRank(newPageRank);
		
		outValue.set(originalNode.toString());
		
		context.write(key, outValue);
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
		context.getCounter(Counter.DELTAS).increment(scaledDelta);
	}
}