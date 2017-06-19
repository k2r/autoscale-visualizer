/**
 * 
 */
package visualizer.draw;

import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public interface IPainter {

	/**
	 * 
	 */
	public void drawTopologyInput();
	
	/**
	 * 
	 */
	public void drawTopologyThroughput();
	
	/**
	 * 
	 */
	public void drawTopologyLosses();
	
	/**
	 * 
	 */
	public void drawTopologyLatency();
	
	/**
	 * 
	 */
	public void drawTopologyNbExecutors();
	
	/**
	 * 
	 */
	public void drawTopologyNbSupervisors();
	
	/**
	 * 
	 */
	public void drawTopologyNbWorkers();
	
	/**
	 * 
	 */
	public void drawTopologyStatus();
	
	/**
	 * 
	 * @param structure
	 */
	public void drawTopologyTraffic(IStructure structure);
	
	/**
	 * 
	 */
	public void drawTopologyRebalancing(IStructure structure);
	
	/**
	 * 
	 * @param component
	 * @param structure
	 */
	public void drawBoltInput(String component, IStructure structure);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltExecuted(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltOutputs(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltLatency(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltCapacity(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltActivity(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltCpuUsage(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltRebalancing(String component);
	
	/**
	 * 
	 * @param component
	 */
	public void drawBoltPending(String component);
}