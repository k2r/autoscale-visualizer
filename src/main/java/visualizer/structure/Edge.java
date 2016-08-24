/**
 * 
 */
package visualizer.structure;

/**
 * @author Roland
 *
 */
public class Edge {

	private String source;
	private String destination;
	
	public Edge(String source, String dest){
		this.source = source;
		this.destination = dest;
	}

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * @return the destination
	 */
	public String getDestination() {
		return destination;
	}

	/**
	 * @param destination the destination to set
	 */
	public void setDestination(String destination) {
		this.destination = destination;
	}
	
	@Override
	public boolean equals(Object o){
		Edge edge = (Edge) o;
		return (edge.getSource().equalsIgnoreCase(this.getSource()) && edge.getDestination().equalsIgnoreCase(this.getDestination()));
	}
}
