import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Text,Text,Text,Text> {
	private Text outKey = new Text();
	private Text outValue  = new Text();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key,value);
		Node node = Node.parseMR(value.toString());
		if(node.getAdjacentNodes() != null && node.getAdjacentNodes().length > 0) {
			//the page rank the node will contribute to the other nodes
			double outboundPageRank = node.getPageRank() / (double)node.getAdjacentNodes().length;
			//go through all the nodes and propagate PageRank to them 
			for (int i = 0; i < node.getAdjacentNodes().length; i++) {
				String neighbor = node.getAdjacentNodes()[i];
				outKey.set(neighbor);
				Node adjacentNode = new Node();
				adjacentNode.setPageRank(outboundPageRank);
				outValue.set(adjacentNode.toString());
				context.write(outKey, outValue);
			}
		}
	}
}