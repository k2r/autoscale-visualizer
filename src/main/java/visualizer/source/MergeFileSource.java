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
public class MergeFileSource implements ISource {

	private String mergeName;
	private ArrayList<String> topologies;
	private ArrayList<String> variations;
	private ArrayList<String> rootDirectories;
	private ArrayList<String> datasetDirectories;
	
	private static final String TOPOLOGY_INPUT = "topology_input";
	private static final String TOPOLOGY_THROUGHPUT = "topology_throughput";
	private static final String TOPOLOGY_DEPHASE = "topology_dephase";
	private static final String TOPOLOGY_LATENCY = "topology_latency";
	private static final String TOPOLOGY_NBEXEC = "topology_nb_executors";
	private static final String TOPOLOGY_NBSUPER = "topology_nb_supervisors";
	private static final String TOPOLOGY_NBWORK = "topology_nb_workers";
	private static final String TOPOLOGY_STATUS = "topology_status";
	private static final String TOPOLOGY_TRAFFIC = "topology_traffic";
	//private static final String TOPOLOGY_REBALANCING = "topology_rebalancing";
	private static final String BOLT_INPUT = "bolt_input";
	private static final String BOLT_EXEC = "bolt_processed";
	private static final String BOLT_OUTPUT = "bolt_output";
	private static final String BOLT_LATENCY = "bolt_latency";
	private static final String BOLT_CAPACITY = "bolt_capacity";
	private static final String BOLT_ACTIVITY = "bolt_activity";
	private static final String BOLT_CPU = "bolt_cpu";
	private static final String BOLT_REBAL = "bolt_rebalancing";
	private static final String BOLT_PENDING = "bolt_pending";
	
	private static final String CAT_TOPOLOGY = "topology";
	private static final String CAT_BOLT = "bolts";
	
	private static final Logger logger = Logger.getLogger("MergeFileSource");
	
	public MergeFileSource(String mergeName, ArrayList<String> topologies, ArrayList<Integer> varCodes) {
		this.mergeName = mergeName;
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
	}
	
	
	public HashMap<String, HashMap<Integer, Double>> getMergedSeries(String category, String dimension, String errorMsg, String component){
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		int nbTopologies = topologies.size();
		HashMap<Integer, Double> dataset = new HashMap<>();
		ArrayList<Integer> timestamps = new ArrayList<>();
		Path pathRef = Paths.get(this.datasetDirectories.get(0) + "/" + category + "/"
				+ component + "_" + dimension + "_" + this.rootDirectories.get(0) + ".csv");
		if(Files.exists(pathRef)){
			try {
				ArrayList<String> data = (ArrayList<String>) Files.readAllLines(pathRef, Charset.defaultCharset());
				for(int j = 1; j < data.size(); j++){
					String[] line = data.get(j).split(";");
					timestamps.add(Integer.parseInt(line[0]));
				}
				
			}catch (IOException e) {
				logger.severe(errorMsg + " because " + e);
			}
		}
		
		ArrayList<Double> values = new ArrayList<>();
		
		for(int i = 0; i < nbTopologies; i++){
			Path path = Paths.get(this.datasetDirectories.get(i) + "/" + category + "/"
					+ component + "_" + dimension + "_" + this.rootDirectories.get(i) + ".csv");
			if(Files.exists(path)){
				try {
					ArrayList<String> data = (ArrayList<String>) Files.readAllLines(path, Charset.defaultCharset());
					for(int j = 1; j < data.size(); j++){
						String[] line = data.get(j).split(";");
						try{
							Integer index = j - 1;
							Double value = values.get(index);
							value += Double.parseDouble(line[1]);
							values.remove(index);
							values.add(index, value);
						}catch(IndexOutOfBoundsException e){
							Double value = Double.parseDouble(line[1]);
							values.add(value);
						}
					}
				} catch (IOException e) {
					logger.severe(errorMsg + " because " + e);
				}
			}
		}
		int nbRecords = timestamps.size();
		for(int k = 0; k < nbRecords; k++){
			Integer avgTimestamp = timestamps.get(k);
			Double avgValue = values.get(k) / nbTopologies;
			dataset.put(avgTimestamp, avgValue);
		}
		alldata.put(this.mergeName, dataset);
		return alldata;
	}
	
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_INPUT, "Unable to merge topologies inputs", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_THROUGHPUT, "Unable to merge topologies throughputs", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyDephase()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyDephase() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_DEPHASE, "Unable to merge topologies dephases", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_LATENCY, "Unable to merge topologies latencies", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyRebalancing(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<String, HashMap<Integer, Double>>> getTopologyRebalancing(IStructure structure) {
		return null;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbExecutors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbExecutors() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_NBEXEC, "Unable to merge topologies executors", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_NBSUPER, "Unable to merge topologies supervisors", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_NBWORK, "Unable to merge topologies workers", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus() {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_STATUS, "Unable to merge topologies status", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure) {
		return getMergedSeries(CAT_TOPOLOGY, TOPOLOGY_TRAFFIC, "Unable to merge topologies traffic", "");
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure) {
		return getMergedSeries(CAT_BOLT, BOLT_INPUT, "Unable to merge bolt " + component + " inputs", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_EXEC, "Unable to merge bolt " + component + " inputs", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_OUTPUT, "Unable to merge bolt " + component + " outputs", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_LATENCY, "Unable to merge bolt " + component + " latencies", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_CAPACITY, "Unable to merge bolt " + component + " capacities", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltPendings(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltPendings(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_PENDING, "Unable to merge bolt " + component + " pendings", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltActivity(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltActivity(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_ACTIVITY, "Unable to merge bolt " + component + " activities", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltCpuUsage(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuUsage(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_CPU, "Unable to merge bolt " + component + " cpu usages", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltRebalancing(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltRebalancing(String component) {
		return getMergedSeries(CAT_BOLT, BOLT_REBAL, "Unable to merge bolt " + component + " rebalancing", component);
	}

}
