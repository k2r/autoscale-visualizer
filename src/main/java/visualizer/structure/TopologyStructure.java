/**
 * 
 */
package visualizer.structure;

import java.util.ArrayList;

/**
 * @author Roland
 *
 */
public class TopologyStructure implements IStructure {

	private ArrayList<Edge> edges;
	private ArrayList<String> components;
	
	public TopologyStructure(ArrayList<Edge> edges) {
		this.edges = edges;
		this.components = new ArrayList<>();
		for(Edge edge : this.edges){
			String source = edge.getSource();
			String destination = edge.getDestination();
			if(!this.components.contains(source)){
				this.components.add(source);
			}
			if(!this.components.contains(destination)){
				this.components.add(destination);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see visualizer.structure.IStructure#getComponents()
	 */
	@Override
	public ArrayList<String> getComponents() {
		return this.components;
	}

	/* (non-Javadoc)
	 * @see visualizer.structure.IStructure#getSpouts()
	 */
	@Override
	public ArrayList<String> getSpouts() {
		ArrayList<String> spouts = new ArrayList<>();
		for(String component : this.getComponents()){
			boolean isSpout = true;
			for(Edge edge : this.edges){
				String destination = edge.getDestination();
				if(destination.equalsIgnoreCase(component)){
					isSpout = false;
					break;
				}
			}
			if(isSpout){
				spouts.add(component);
			}
		}
		return spouts;
	}

	/* (non-Javadoc)
	 * @see visualizer.structure.IStructure#getBolts()
	 */
	@Override
	public ArrayList<String> getBolts() {
		ArrayList<String> bolts = new ArrayList<>();
		for(String component : this.getComponents()){
			boolean isBolt = false;
			for(Edge edge : this.edges){
				String destination = edge.getDestination();
				if(destination.equalsIgnoreCase(component)){
					isBolt = true;
					break;
				}
			}
			if(isBolt){
				bolts.add(component);
			}
		}
		return bolts;
	}

	/* (non-Javadoc)
	 * @see visualizer.structure.IStructure#getParents(java.lang.String)
	 */
	@Override
	public ArrayList<String> getParents(String component) {
		ArrayList<String> parents = new ArrayList<>();
		for(Edge edge : this.edges){
			String destination = edge.getDestination();
			if(destination.equalsIgnoreCase(component)){
				String source = edge.getSource();
				parents.add(source);
			}
		}
		return parents;
	}

	@Override
	public ArrayList<String> getChildren(String component) {
		ArrayList<String> children = new ArrayList<>();
		for (Edge edge : this.edges){
			String source = edge.getSource();
			if(source.equalsIgnoreCase(component)){
				String destination = edge.getDestination();
				children.add(destination);
			}
		}
		return children;
	}
}