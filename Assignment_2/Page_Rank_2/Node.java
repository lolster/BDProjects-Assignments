import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {
	double pageRank = 1;
	String[] adjacentNodes;
	private static final String fieldSep = "\t";

	public Node() {}
	
	public Node(double pageRank, String[] adjacentNodes) {
		this.pageRank = pageRank;
		this.adjacentNodes = adjacentNodes;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getAdjacentNodes() {
		return adjacentNodes;
	}

	public void setAdjacentNodes(String[] adjacentNodes) {
		this.adjacentNodes = adjacentNodes;
	}

	public boolean containsAdjacentNodes() {
		return adjacentNodes != null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);

		if (getAdjacentNodes() != null) {
			sb.append(fieldSep)
					.append(StringUtils
							.join(getAdjacentNodes(), fieldSep));
		}
		return sb.toString();
	}

	public static Node parseMR(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(
				value, fieldSep);
		if (parts.length < 1) {
			throw new IOException(
					"Expected 1 or more parts but received " + parts.length);
		}
		Node node = new Node();
		node.setPageRank(Double.valueOf(parts[0]));
		if (parts.length > 1) {
			node.setAdjacentNodes(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}

}