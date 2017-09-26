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
	public HashMap<String, HashMap<Integer, Double>> getTopologyDephase();
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency();
	
	@Deprecated
	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<String, HashMap<Integer, Double>>> getTopologyRebalancing(IStructure structure);
	
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
	
	@Deprecated
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
	public HashMap<String, HashMap<Integer, Double>> getBoltPendings(String component);
	
	@Deprecated
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltActivity(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuUsage(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltRebalancing(String component);
	
	/**
	 * 
	 * @param component
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuStdDev(String component);
}