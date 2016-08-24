/**
 * 
 */
package visualizer.source;

import java.util.HashMap;

import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public interface ISource {

	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyInput();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyThroughput();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyLosses();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyLatency();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyNbExecutors();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyNbSupervisors();
	
	/**
	 * 
	 * @param structure
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyNbWorkers();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyStatus();
	
	/**
	 * 
	 * @param structure
	 * @return
	 */
	public HashMap<Integer, Double> getTopologyTraffic(IStructure structure);
	
	/**
	 * 
	 * @param component
	 * @param structure
	 * @return
	 */
	public HashMap<Integer, Double> getBoltInput(String component, IStructure structure);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<Integer, Double> getBoltExecuted(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<Integer, Double> getBoltOutputs(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<Integer, Double> getBoltLatency(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<Integer, Double> getBoltProcessingRate(String component);

	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<Integer, Double> getBoltEPR(String component);
}