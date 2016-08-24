/**
 * 
 */
package visualizer.structure;

import java.util.ArrayList;

/**
 * @author Roland
 *
 */
public interface IStructure {

	/**
	 * 
	 * @return
	 */
	public ArrayList<String> getComponents();
	
	/**
	 * 
	 * @return
	 */
	public ArrayList<String> getSpouts();
	
	/**
	 * 
	 * @return
	 */
	public ArrayList<String> getBolts();
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public ArrayList<String> getParents(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public ArrayList<String> getChildren(String component);

}
