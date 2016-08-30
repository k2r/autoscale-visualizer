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

	private String topology1;
	private String variation1;
	
	private String topology2;
	private String variation2;
	
	private String rootDirectory1;
	private String datasetDirectory1;
	
	private String rootDirectory2;
	private String datasetDirectory2;
	
	private final static String TOPO_DIR = "topology";
	private final static String BOLT_DIR = "bolts";
	private static final String TOPOLOGY_INPUT = "topology_input";
	private static final String TOPOLOGY_THROUGHPUT = "topology_throughput";
	private static final String TOPOLOGY_LOSSES = "topology_losses";
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
	private static final String BOLT_PROCRATE = "bolt_processing_rate";
	private static final String BOLT_EPR = "bolt_epr";
	
	private static final Logger logger = Logger.getLogger("FileSource");
	
	public FileSource(String topology1, Integer varCode1, String topology2, Integer varCode2) {
		this.topology1 = topology1;
		this.topology2 = topology2;
		
		switch(varCode1){
		case(1): this.variation1 = "linear_increase";
				break;
		case(2): this.variation1 = "scale_increase";
				break;
		case(3): this.variation1 = "exponential_increase";
				break;
		case(4): this.variation1 = "logarithmic_increase";
				break;
		case(5): this.variation1 = "linear_decrease";
				break;
		case(6): this.variation1 = "scale_decrease";
				break;
		case(7): this.variation1 = "exponential_decrease";
				break;
		case(8): this.variation1 = "all_variations";
				break;
		case(9): this.variation1 = "no_variation";
				break;
		}
		
		switch(varCode2){
		case(1): this.variation2 = "linear_increase";
				break;
		case(2): this.variation2 = "scale_increase";
				break;
		case(3): this.variation2 = "exponential_increase";
				break;
		case(4): this.variation2 = "logarithmic_increase";
				break;
		case(5): this.variation2 = "linear_decrease";
				break;
		case(6): this.variation2 = "scale_decrease";
				break;
		case(7): this.variation2 = "exponential_decrease";
				break;
		case(8): this.variation2 = "all_variations";
				break;
		case(9): this.variation2 = "no_variation";
				break;
		}
		
		this.rootDirectory1 = this.topology1 + "_" + this.variation1;
		this.datasetDirectory1 = this.rootDirectory1 + "/datasets";
		
		this.rootDirectory2 = this.topology2 + "_" + this.variation2;
		this.datasetDirectory2 = this.rootDirectory2 + "/datasets";
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Input = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_INPUT + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Input)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Input, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + " inputs because " + e);
			}
		}
		Path topology2Input = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_INPUT + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Input)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Input, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + " inputs because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Throughput = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_THROUGHPUT + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Throughput)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Throughput, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " throughput because " + e);
			}
		}
		Path topology2Throughput = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_THROUGHPUT + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Throughput)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Throughput, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " throughput because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLosses()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLosses() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Losses = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_LOSSES + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Losses)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Losses, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " losses because " + e);
			}
		}
		Path topology2Losses = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_LOSSES + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Losses)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Losses, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " losses because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Latency = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_LATENCY + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Latency)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Latency, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " latency because " + e);
			}
		}
		Path topology2Latency = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_LATENCY + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Latency)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Latency, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " latency because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbExecutors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbExecutors() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1NbExec = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBEXEC + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1NbExec)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1NbExec, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " executors because " + e);
			}
		}
		Path topology2NbExec = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBEXEC + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2NbExec)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2NbExec, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " executors because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1NbSup = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBSUPER + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1NbSup)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1NbSup, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " supervisors because " + e);
			}
		}
		Path topology2NbSup = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBSUPER + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2NbSup)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2NbSup, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " supervisors because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1NbWorkers = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBWORK + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1NbWorkers)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1NbWorkers, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " workers because " + e);
			}
		}
		Path topology2NbWorkers = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_NBWORK + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2NbWorkers)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2NbWorkers, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " workers because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Status = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_STATUS + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Status)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Status, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " status because " + e);
			}
		}
		Path topology2Status = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_STATUS + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Status)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Status, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " status because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Traffic = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_TRAFFIC + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Traffic)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Traffic, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " traffic because " + e);
			}
		}
		Path topology2Traffic = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_TRAFFIC + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Traffic)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Traffic, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " traffic because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1Input = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_INPUT + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1Input)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1Input, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " input because " + e);
			}
		}
		Path bolt2Input = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_INPUT + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2Input)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2Input, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " input because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1Exec = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_EXEC + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1Exec)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1Exec, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " executed tuples because " + e);
			}
		}
		Path bolt2Exec = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_EXEC + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2Exec)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2Exec, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " executed tuples because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1Output = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_OUTPUT + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1Output)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1Output, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " output because " + e);
			}
		}
		Path bolt2Output = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_OUTPUT + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2Output)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2Output, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " output because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1Latency = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_LATENCY + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1Latency)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1Latency, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " latency because " + e);
			}
		}
		Path bolt2Latency = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_LATENCY + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2Latency)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2Latency, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " latency because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1ProcRate = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_PROCRATE + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1ProcRate)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1ProcRate, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " processing rate because " + e);
			}
		}
		Path bolt2Input = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_PROCRATE + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2Input)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2Input, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " processing rate because " + e);
			}
		}
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltEPR(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltEPR(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path bolt1EPR = Paths.get(this.datasetDirectory1 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_EPR + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(bolt1EPR)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(bolt1EPR, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology1 + "." + component + " epr because " + e);
			}
		}
		Path bolt2EPR = Paths.get(this.datasetDirectory2 + "/" + BOLT_DIR + "/" 
				 + component + "_" + BOLT_EPR + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(bolt2EPR)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(bolt2EPR, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve " + this.topology2 + "." + component + " epr because " + e);
			}
		}
		return alldata;
	}

	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyRebalancing() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		Path topology1Scales = Paths.get(this.datasetDirectory1 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_REBALANCING + "_" + this.rootDirectory1 + ".csv");
		if(Files.exists(topology1Scales)){
			try {
				HashMap<Integer, Double> dataset1 = new HashMap<>();
				ArrayList<String> data1 = (ArrayList<String>) Files.readAllLines(topology1Scales, Charset.defaultCharset());
				for(int i = 1; i < data1.size(); i++){
					String[] line = data1.get(i).split(";");
					dataset1.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology1, dataset1);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology1 + " scaling actions because " + e);
			}
		}
		Path topology2Scales = Paths.get(this.datasetDirectory2 + "/" + TOPO_DIR 
				+ "/" + TOPOLOGY_REBALANCING + "_" + this.rootDirectory2 + ".csv");
		if(Files.exists(topology2Scales)){
			try {
				HashMap<Integer, Double> dataset2 = new HashMap<>();
				ArrayList<String> data2 = (ArrayList<String>) Files.readAllLines(topology2Scales, Charset.defaultCharset());
				for(int i = 1; i < data2.size(); i++){
					String[] line = data2.get(i).split(";");
					dataset2.put(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
				}
				alldata.put(this.topology2, dataset2);
			} catch (IOException e) {
				logger.severe("Unable to retrieve "+ this.topology2 + " scaling actions because " + e);
			}
		}
		return alldata;
	}
}