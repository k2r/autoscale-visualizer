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
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyLosses();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency();
	
	//TODO change the signature of this method into HashMap<String (topology) <HashMap <Integer (timestamp) <HashMap <String (component), Double (Number of executors)>>>
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyRebalancing();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbExecutors();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors();
	
	/**
	 * 
	 * @param structure
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus();
	
	/**
	 * 
	 * @param structure
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure);
	
	/**
	 * 
	 * @param component
	 * @param structure
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component);

	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltEPR(String component);
}