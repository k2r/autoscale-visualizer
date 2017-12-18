/**
 * 
 */
package visualizer.source;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public class FileSource implements ISource {

	private ArrayList<String> topologies;
	private ArrayList<String> variations;
	private ArrayList<String> rootDirectories;
	private ArrayList<String> datasetDirectories;
	
	private Integer minTimestamp;
	private Integer maxTimestamp;
	
	private static final String TOPOLOGY_INPUT = "topology_input";
	private static final String TOPOLOGY_THROUGHPUT = "topology_throughput";
	private static final String TOPOLOGY_DEPHASE = "topology_dephase";
	private static final String TOPOLOGY_LATENCY = "topology_latency";
	private static final String TOPOLOGY_NBEXEC = "topology_nb_executors";
	private static final String TOPOLOGY_NBSUPER = "topology_nb_supervisors";
	private static final String TOPOLOGY_NBWORK = "topology_nb_workers";
	private static final String TOPOLOGY_STATUS = "topology_status";
	private static final String TOPOLOGY_TRAFFIC = "topology_traffic";
	private static final String TOPOLOGY_REBALANCING = "topology_rebalancing";
	private static final String BOLT_INPUT = "bolt_input";
	private static final String BOLT_EXEC = "bolt_processed";
	private static final String BOLT_OUTPUT = "bolt_output";
	private static final String BOLT_LATENCY = "bolt_latency";
	private static final String BOLT_CAPACITY = "bolt_capacity";
	private static final String BOLT_ACTIVITY = "bolt_activity";
	private static final String BOLT_CPU = "bolt_cpu";
	private static final String BOLT_STD_CPU = "bolt_stdDev_cpu";
	private static final String BOLT_REBAL = "bolt_rebalancing";
	private static final String BOLT_PENDING = "bolt_pending";
	
	private static final String CAT_TOPOLOGY = "topology";
	private static final String CAT_BOLT = "bolts";
	
	private static final Logger logger = Logger.getLogger("FileSource");
	
	public FileSource(ArrayList<String> topologies, ArrayList<Integer> varCodes, Integer minTimestamp, Integer maxTimestamp) {
		this.topologies = topologies;
		this.variations = new ArrayList<>();
		for(int i = 0; i < varCodes.size(); i++){
			Integer varCode = varCodes.get(i);
			switch(varCode){
			case(1): this.variations.add("linear_increase");
			break;
			case(2): this.variations.add("scale_increase");
			break;
			case(3): this.variations.add("exponential_increase");
			break;
			case(4): this.variations.add("logarithmic_increase");
			break;
			case(5): this.variations.add("linear_decrease");
			break;
			case(6): this.variations.add("scale_decrease");
			break;
			case(7): this.variations.add("exponential_decrease");
			break;
			case(8): this.variations.add("all_variations");
			break;
			case(9): this.variations.add("no_variation");
			break;
			}
		}
		this.rootDirectories = new ArrayList<>();
		this.datasetDirectories = new ArrayList<>();
		for(int i = 0; i < topologies.size(); i++){
			this.rootDirectories.add(this.topologies.get(i) + "_" + this.variations.get(0));
			this.datasetDirectories.add(this.rootDirectories.get(i) + "/datasets");
		}
		this.minTimestamp = minTimestamp;
		this.maxTimestamp = maxTimestamp;
	}
	
	
	public HashMap<String, HashMap<Integer, Double>> getSeries(String category, String dimension, String errorMsg, String component){
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		int nbTopologies = topologies.size();
		for(int i = 0; i < nbTopologies; i++){
			Path path = Paths.get(this.datasetDirectories.get(i) + "/" + category 
					+ "/" + component + "_" + dimension + "_" + this.rootDirectories.get(i) + ".csv");
			if(Files.exists(path)){
				try {
					HashMap<Integer, Double> dataset = new HashMap<>();
					ArrayList<String> data = (ArrayList<String>) Files.readAllLines(path, Charset.defaultCharset());
					for(int j = 1; j < data.size(); j++){
						String[] line = data.get(j).split(";");
						Integer timestamp = Integer.parseInt(line[0]);
						Double avgValue = Double.parseDouble(line[1]);
						if(this.minTimestamp <= timestamp && this.maxTimestamp > timestamp){
							dataset.put(timestamp, avgValue);
						}
					}
					alldata.put(this.topologies.get(i), dataset);
				} catch (IOException e) {
					logger.severe(errorMsg + " " + e);
				}
			}
		}
		return alldata;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_INPUT, "Unable to retrieve topology inputs because", "");	
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_THROUGHPUT, "Unable to retrieve topology throughput because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLosses()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyDephase() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_DEPHASE, "Unable to retrieve topology losses because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_LATENCY, "Unable to retrieve topology latency because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbExecutors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbExecutors() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_NBEXEC, "Unable to retrieve the number of executors for the topology because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_NBSUPER, "Unable to retrieve the number of supervisors for the topology because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_NBWORK, "Unable to retrieve the number of supervisors for the topology because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus() {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_STATUS, "Unable to retrieve topology status because", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure) {
		return getSeries(CAT_TOPOLOGY, TOPOLOGY_TRAFFIC, "Unable to retrieve topology traffic because", "");
	}

	@Override
	public HashMap<String, HashMap<String, HashMap<Integer, Double>>> getTopologyRebalancing(IStructure structure) {
		HashMap<String, HashMap<String, HashMap<Integer, Double>>> alldata = new HashMap<>();
		int nbTopologies = topologies.size();
		for(int i = 0; i < nbTopologies; i++){
			Path topologyScales = Paths.get(this.datasetDirectories.get(i) + "/" + CAT_TOPOLOGY 
					+ "/_" + TOPOLOGY_REBALANCING + "_" + this.rootDirectories.get(i) + ".csv");
			if(Files.exists(topologyScales)){
				try {
					HashMap<String, HashMap<Integer, Double>> dataset = new HashMap<>();
					ArrayList<String> data = (ArrayList<String>) Files.readAllLines(topologyScales, Charset.defaultCharset());

					ArrayList<String> bolts = structure.getBolts();
					for(String bolt : bolts){
						HashMap<Integer, Double> componentInfo = new HashMap<>();
						for(int j = 1; j < data.size(); j++){
							String[] line = data.get(j).split(";");
							Integer timestamp = Integer.parseInt(line[0]);
							String[] actions = line[1].split(":");
							int nbActions = actions.length;

							for(int k = 0; k < nbActions; k++){
								String[] action = actions[k].split("@");
								String component = action[0];
								Double nbExecutors = Double.parseDouble(action[1]);
								if(component.equalsIgnoreCase(bolt)){
									componentInfo.put(timestamp, nbExecutors);
								}
							}
						}
						dataset.put(bolt, componentInfo);
					}
					alldata.put(this.topologies.get(i), dataset);
				} catch (IOException e) {
					logger.severe("Unable to retrieve "+ this.topologies.get(i) + " scaling actions because " + e);
				}
			}
		}
		return alldata;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure) {
		return getSeries(CAT_BOLT, BOLT_INPUT, "Unable to retrieve bolt inputs because", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component) {
		return getSeries(CAT_BOLT, BOLT_EXEC, "Unable to retrieve the number of executed stream element for the bolt because", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component) {
		return getSeries(CAT_BOLT, BOLT_OUTPUT, "Unable to retrieve bolt outputs because", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component) {
		return getSeries(CAT_BOLT, BOLT_LATENCY, "Unable to retrieve bolt latency because", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component) {
		return getSeries(CAT_BOLT, BOLT_CAPACITY, "Unable to retrieve bolt processing rate because", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltEPR(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltActivity(String component) {
		return getSeries(CAT_BOLT, BOLT_ACTIVITY, "Unable to retrieve bolt activity because", component);
	}


	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuUsage(String component) {
		return getSeries(CAT_BOLT, BOLT_CPU, "Unable to retrieve CPU usage for bolt because", component);
	}


	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltRebalancing(String component) {
		return getSeries(CAT_BOLT, BOLT_REBAL, "Unable to retrieve modification of parallelism degree for bolt because", component);
	}


	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltPendings(String component) {
		return getSeries(CAT_BOLT, BOLT_PENDING, "Unable to retrieve the number of pending stream elements for bolt because", component);
	}


	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuStdDev(String component) {
		return getSeries(CAT_BOLT, BOLT_STD_CPU, "Unable to retrieve standard deviation of CPU usage for bolt because", component);
	}	
}