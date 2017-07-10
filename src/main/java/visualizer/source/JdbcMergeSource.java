/**
 * 
 */
package visualizer.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import visualizer.structure.IStructure;
import visualizer.util.Utils;

/**
 * @author Roland
 *
 */
public class JdbcMergeSource implements ISource {

	private String topology;
	private final Connection connection;
	 
	private final static Integer STEP = 10;
	
	private final static String TABLE_SPOUT = "all_time_spouts_stats";
	private final static String TABLE_BOLT = "all_time_bolts_stats";
	//private final static String TABLE_STATUS = "topologies_status";
	private final static String TABLE_ESTIM = "operators_estimation";
	//private final static String TABLE_SCALE = "scales";
	
	private final static String COL_TIMESTAMP = "timestamp";
	private final static String COL_HOST = "host";
	private final static String COL_PORT = "port";
	private final static String COL_TOPOLOGY = "topology";
	private final static String COL_COMPONENT = "component";
	private final static String COL_START_TASK = "start_task";
	private final static String COL_UPDT_EXEC = "update_executed";
	private final static String COL_UPDT_OUTPUT = "update_outputs";
	private final static String COL_UPDT_THROUGHPUT = "update_throughput";
	private final static String COL_UPDT_LOSS = "update_losses";
	private final static String COL_AVG_LATENCY = "execute_ms_avg";
	private final static String COL_AVG_COMPLETE_LATENCY = "complete_ms_avg";
	//private final static String COL_STATUS = "status";
	private final static String COL_CPU = "cpu_usage";
	private final static String COL_PENDING = "pending";
	
	private static final Logger logger = Logger.getLogger("JdbcMergeSource");
	
	public JdbcMergeSource(String dbHost, String dbName, String dbUser, String dbPwd, String topology) throws ClassNotFoundException, SQLException {
		this.topology = topology;
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/" + dbName;
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
	}
	
	/**
	 * This method takes as input a collection of timestamped records for several topologies and group them to compute min, max and average series for all topologies 
	 * @param table the table including target column
	 * @param aggregate the aggregate operator to apply on groups: SUM(, AVG(, MAX(, COUNT(DISTINCT ...
	 * @param targetColumn the column including values to group and collect
	 * @return a map including maps representing min, max and average series
	 */
	public HashMap<String, HashMap<Integer, Double>> getTopologyAggregatedInfo(String aggregate, String targetColumn){
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashMap<Integer, Double> iteration = new HashMap<>(); 
		HashSet<Integer> timestamps = new HashSet<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_TOPOLOGY + ", " + aggregate + targetColumn + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP; 
		Statement statement;
		try {
			Integer timestamp = 0;
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					timestamp = 0;
				}
				Double targetValue = result.getDouble(aggregate + targetColumn + ")");
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of " + targetColumn + " because " + e);
		}
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}
	
	public HashMap<String, HashMap<Integer, Double>> getBoltAggregatedInfo(String table, String component, String aggregate, String targetColumn){
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashMap<Integer, Double> iteration = new HashMap<>(); 
		HashSet<Integer> timestamps = new HashSet<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_TOPOLOGY + ", " + aggregate + targetColumn + ") " +
				" FROM " + table + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'" +
				" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_COMPONENT; 
		Statement statement;
		try {
			Integer timestamp = 0;
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					timestamp = 0;
				}
				Double targetValue = result.getDouble(aggregate + targetColumn + ")");
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of " + targetColumn + " of bolt " + component + " because " + e);
		}
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput() {
		return getTopologyAggregatedInfo("SUM(", COL_UPDT_OUTPUT);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput() {
		return getTopologyAggregatedInfo("SUM(", COL_UPDT_THROUGHPUT);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyDephase()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyDephase() {
		return getTopologyAggregatedInfo("SUM(", COL_UPDT_LOSS);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency() {
		return getTopologyAggregatedInfo("MAX(", COL_AVG_COMPLETE_LATENCY);
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
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashMap<Integer, Double> iteration = new HashMap<>(); 
		HashSet<Integer> timestamps = new HashSet<>();
		String querySpout = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP;  
		Statement statementSpout;
		try {
			Integer timestamp = 0;
			statementSpout = this.connection.createStatement();
			ResultSet result = statementSpout.executeQuery(querySpout);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					timestamp = 0;
				}
				Double targetValue = result.getDouble("COUNT(DISTINCT " + COL_START_TASK + ")");
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of " + COL_START_TASK + " because " + e);
		}
		String queryBolt = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") " +
				" FROM " + TABLE_BOLT + 
				" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP; 
		Statement statementBolt;
		try {
			Integer timestamp = 0;
			statementBolt = this.connection.createStatement();
			ResultSet result = statementBolt.executeQuery(queryBolt);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					iteration = iterations.get(prevTopology);
					timestamp = 0;
				}
				Double targetValue = result.getDouble("COUNT(DISTINCT " + COL_START_TASK + ")");
				if(iteration.containsKey(timestamp)){
					targetValue += iteration.get(timestamp);
					iteration.remove(timestamp);
				}
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of " + COL_START_TASK + " because " + e);
		}
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashMap<Integer, Double> iteration = new HashMap<>(); 
		HashSet<Integer> timestamps = new HashSet<>();
		String query = "SELECT C." + COL_TOPOLOGY + ", C." + COL_TIMESTAMP + ", COUNT(DISTINCT C." + COL_HOST + ") " +
				" FROM(" +
				" SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST +
				" FROM " + TABLE_SPOUT + 
				" UNION " + 
				" SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST +
				" FROM " + TABLE_BOLT +  
				") C" + 
				" GROUP BY C." + COL_TOPOLOGY + ", C." + COL_TIMESTAMP;
		Statement statement;
		try {
			Integer timestamp = 0;
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					timestamp = 0;
				}
				Double targetValue = result.getDouble("COUNT(DISTINCT C." + COL_HOST + ")");
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of " + COL_HOST + " because " + e);
		}
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashMap<Integer, Double> iteration = new HashMap<>(); 
		HashSet<Integer> timestamps = new HashSet<>();
		String query = "SELECT D." + COL_TOPOLOGY + ", D." + COL_TIMESTAMP + ", SUM(D.nbport) AS nbWorkers " +
				" FROM(" +
				" SELECT C." + COL_TOPOLOGY + ", C." + COL_TIMESTAMP + ", COUNT(DISTINCT C." + COL_PORT + ") AS nbport" +
				" FROM(" +
				" SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT +
				" FROM " + TABLE_SPOUT + 
				" UNION " + 
				" SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT +
				" FROM " + TABLE_BOLT +  
				") C" + 
				" GROUP BY C."  + COL_TOPOLOGY + ", C." + COL_TIMESTAMP + ", C." + COL_HOST + ") D" +
				" GROUP BY D."  + COL_TOPOLOGY + ", D." + COL_TIMESTAMP;
		Statement statement;
		try {
			Integer timestamp = 0;
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			String prevTopology = "";
			while(result.next()){
				String currTopology = result.getString(COL_TOPOLOGY);
				if(!prevTopology.equalsIgnoreCase(currTopology)){
					if(!prevTopology.equalsIgnoreCase("")){
						iterations.put(prevTopology, iteration);
						iteration = new HashMap<>();
					}
					prevTopology = currTopology;
					timestamp = 0;
				}
				Double targetValue = result.getDouble("nbWorkers");
				iteration.put(timestamp, targetValue);
				timestamps.add(timestamp);
				timestamp += STEP;
			}
			iterations.put(prevTopology, iteration);
		} catch (SQLException e) {
			logger.severe("Unable to recover aggregate values of nbWorkers because " + e);
		}
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus() {
		return null;
	}

	
	public HashMap<Integer, Double> getTopologyTraffic(String topology, IStructure structure){
		HashMap<Integer, Double> dataSet = new HashMap<>();
		ArrayList<String> bolts = structure.getBolts();
		for(String bolt : bolts){
			String queryBolt = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
					" FROM " + TABLE_BOLT +
					" WHERE " + COL_TOPOLOGY  + " = '" + topology + "' AND " + COL_COMPONENT + " = '" + bolt + "' " +
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
			Statement statementBolt;
			try {
				statementBolt = this.connection.createStatement();
				ResultSet resultBolt = statementBolt.executeQuery(queryBolt);
				HashMap<Integer, HashMap<String, Double>> boltOutputPerHost = new HashMap<>();
				while(resultBolt.next()){
					Integer timestamp = resultBolt.getInt(COL_TIMESTAMP);
					String host = resultBolt.getString(COL_HOST);
					Double output = resultBolt.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
					if(!boltOutputPerHost.containsKey(timestamp)){
						HashMap<String, Double> hostOutput = new HashMap<>();
						hostOutput.put(host, output);
						boltOutputPerHost.put(timestamp, hostOutput);
					}
					HashMap<String, Double> hostOutput = boltOutputPerHost.get(timestamp);
					hostOutput.put(host, output);
					boltOutputPerHost.remove(timestamp);
					boltOutputPerHost.put(timestamp, hostOutput);
				}
				ArrayList<String> children = structure.getChildren(bolt);
				for(String child : children){
					String queryChild = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
							" FROM " + TABLE_BOLT +
							" WHERE " + COL_TOPOLOGY  + " = '" + topology + "' AND " + COL_COMPONENT + " = '" + child + "' " +
							" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
					Statement statementChild;
					try {
						statementChild = this.connection.createStatement();
						ResultSet resultChild = statementChild.executeQuery(queryChild);
						HashMap<Integer, HashMap<String, Double>> childOutputPerHost = new HashMap<>();
						while(resultChild.next()){
							Integer timestamp = resultChild.getInt(COL_TIMESTAMP);
							String host = resultChild.getString(COL_HOST);
							Double output = resultChild.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
							if(!childOutputPerHost.containsKey(timestamp)){
								HashMap<String, Double> hostOutput = new HashMap<>();
								hostOutput.put(host, output);
								childOutputPerHost.put(timestamp, hostOutput);
							}
							HashMap<String, Double> hostOutput = childOutputPerHost.get(timestamp);
							hostOutput.put(host, output);
							childOutputPerHost.remove(timestamp);
							childOutputPerHost.put(timestamp, hostOutput);
						}
						HashMap<Integer, Integer> normTimestamps = new HashMap<>();
						ArrayList<Integer> rawTimestamps = new ArrayList<>();
						rawTimestamps.addAll(boltOutputPerHost.keySet());
						Collections.sort(rawTimestamps);
						int size = rawTimestamps.size();
						int refTimestamp = 0;
						for(int i = 0; i < size; i++){
							normTimestamps.put(rawTimestamps.get(i), refTimestamp);
							refTimestamp += STEP;
						}
						for(Integer t : boltOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = boltOutputPerHost.get(t);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(t);
							//Let define to how many supervisors the bolt must distribute tuples
							Set<String> boltHosts = bolthostOutput.keySet();
							if(childHostOuput != null){
								Integer childParalDegree = childHostOuput.keySet().size();
								for(String host : boltHosts){
									traffic += bolthostOutput.get(host);
									if(childHostOuput.containsKey(host)){
										traffic = traffic - (bolthostOutput.get(host) / childParalDegree);
										
									}
								}
							}
							int normT = normTimestamps.get(t);
							if(!dataSet.containsKey(normT)){
								dataSet.put(normT, 0.0);
							}
							Double globalTraffic = dataSet.get(normT);
							globalTraffic += traffic;
							dataSet.remove(normT);
							dataSet.put(normT, globalTraffic);
						}
					}catch (SQLException e) {
						logger.severe("Unable to recover topology traffic because " + e);
					}
				}
			}catch (SQLException e) {
				logger.severe("Unable to recover topology traffic because " + e);
			}
		}
		ArrayList<String> spouts = structure.getSpouts();
		for(String spout : spouts){
			String querySpout = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
					" FROM " + TABLE_SPOUT +
					" WHERE " + COL_TOPOLOGY  + " = '" + topology + "' AND " + COL_COMPONENT + " = '" + spout + "'" + 
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
			Statement statementSpout;
			try {
				statementSpout = this.connection.createStatement();
				ResultSet resultSpout = statementSpout.executeQuery(querySpout);
				HashMap<Integer, HashMap<String, Double>> spoutOutputPerHost = new HashMap<>();
				while(resultSpout.next()){
					Integer timestamp = resultSpout.getInt(COL_TIMESTAMP);
					String host = resultSpout.getString(COL_HOST);
					Double output = resultSpout.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
					if(!spoutOutputPerHost.containsKey(timestamp)){
						HashMap<String, Double> hostOutput = new HashMap<>();
						hostOutput.put(host, output);
						spoutOutputPerHost.put(timestamp, hostOutput);
					}
					HashMap<String, Double> hostOutput = spoutOutputPerHost.get(timestamp);
					hostOutput.put(host, output);
					spoutOutputPerHost.remove(timestamp);
					spoutOutputPerHost.put(timestamp, hostOutput);
				}
				ArrayList<String> children = structure.getChildren(spout);
				for(String child : children){
					String queryChild = "SELECT " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
							" FROM " + TABLE_BOLT +
							" WHERE " + COL_TOPOLOGY  + " = '" + topology + "' AND " + COL_COMPONENT + " = '" + child + "'" + 
							" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
					Statement statementChild;
					try {
						statementChild = this.connection.createStatement();
						ResultSet resultChild = statementChild.executeQuery(queryChild);
						HashMap<Integer, HashMap<String, Double>> childOutputPerHost = new HashMap<>();
						while(resultChild.next()){
							Integer timestamp = resultChild.getInt(COL_TIMESTAMP);
							String host = resultChild.getString(COL_HOST);
							Double output = resultChild.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
							if(!childOutputPerHost.containsKey(timestamp)){
								HashMap<String, Double> hostOutput = new HashMap<>();
								hostOutput.put(host, output);
								childOutputPerHost.put(timestamp, hostOutput);
							}
							HashMap<String, Double> hostOutput = childOutputPerHost.get(timestamp);
							hostOutput.put(host, output);
							childOutputPerHost.remove(timestamp);
							childOutputPerHost.put(timestamp, hostOutput);
						}
						HashMap<Integer, Integer> normTimestamps = new HashMap<>();
						ArrayList<Integer> rawTimestamps = new ArrayList<>();
						rawTimestamps.addAll(spoutOutputPerHost.keySet());
						Collections.sort(rawTimestamps);
						int size = rawTimestamps.size();
						int refTimestamp = 0;
						for(int i = 0; i < size; i++){
							normTimestamps.put(rawTimestamps.get(i), refTimestamp);
							refTimestamp += STEP;
						}
						for(Integer t : spoutOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = spoutOutputPerHost.get(t);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(t);
							Set<String> boltHosts = bolthostOutput.keySet();
							if(childHostOuput != null){
								for(String host : boltHosts){
									if(!childHostOuput.containsKey(host)){
										traffic += bolthostOutput.get(host);
									}
								}
							}
							int normT = normTimestamps.get(t);
							if(!dataSet.containsKey(normT)){
								dataSet.put(normT, 0.0);
							}
							Double globalTrafic = dataSet.get(normT);
							globalTrafic += traffic;
							dataSet.remove(normT);
							dataSet.put(normT, globalTrafic);
						}
					}catch (SQLException e) {
						logger.severe("Unable to recover topology traffic because " + e);
					}
				}
			}catch (SQLException e) {
				logger.severe("Unable to recover topology traffic because " + e);
			}
		}
		return dataSet;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashSet<Integer> timestamps = new HashSet<>();
		HashSet<String> topologyNames = new HashSet<>();
		//Recover the name of each topology in the record collection to merge
		String queryTopologies = "SELECT DISTINCT " + COL_TOPOLOGY +
									" FROM " + TABLE_SPOUT + ";";//spout and bolt tables should contains all topology names but spout table contains generally less records
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(queryTopologies);
			while(result.next()){
				String topology = result.getString(COL_TOPOLOGY);
				topologyNames.add(topology);
			}
		} catch (SQLException e) {
			logger.severe("Unable to extract topologies to merge (network traffic) because " + e);
		}
		//For each topology compute the traffic and add it to the iteration set. Also add recorded timestamps to the timestamp set
		for(String topology : topologyNames){
			HashMap<Integer, Double> iteration = this.getTopologyTraffic(topology, structure);
			iterations.put(topology, iteration);
			timestamps.addAll(iteration.keySet());
		}
		//Compute min, average and max series
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}

	public HashMap<Integer, Double> getBoltInput(String component, String topology, IStructure structure){
		HashMap<Integer, Double> dataSet = new HashMap<>();
		ArrayList<String> parents = structure.getParents(component);
		HashSet<Integer> rawTimestamps = new HashSet<>();
		for(String parent : parents){
			String queryBolt = "SELECT " + COL_TOPOLOGY  + ", " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
					" FROM " + TABLE_BOLT + 
					" WHERE " + COL_TOPOLOGY  + " = '" + topology +  "' AND " +  COL_COMPONENT + " = '" + parent + "'" +
					" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", "  + COL_COMPONENT ; 
			Statement statementBolt;
			try {
				statementBolt = this.connection.createStatement();
				ResultSet result = statementBolt.executeQuery(queryBolt);
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP);
					rawTimestamps.add(timestamp);
					Double parentBoltNbOutput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
					if(dataSet.containsKey(timestamp)){
						parentBoltNbOutput += dataSet.get(timestamp);
						dataSet.remove(timestamp);
					}
					dataSet.put(timestamp, parentBoltNbOutput);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover the number of emitted tuples for bolt " + component + " because " + e);
			}
			
			String querySpout = "SELECT " + COL_TOPOLOGY  + ", " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
					" FROM " + TABLE_SPOUT + 
					" WHERE " + COL_TOPOLOGY  + " = '" + topology +  "' AND " +  COL_COMPONENT + " = '" + parent + "'" +
					" GROUP BY " + COL_TOPOLOGY + ", " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", "  + COL_COMPONENT ; 
			Statement statementSpout;
			try {
				statementSpout = this.connection.createStatement();
				ResultSet result = statementSpout.executeQuery(querySpout);
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP);
					rawTimestamps.add(timestamp);
					Double parentSpoutNbOutput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
					if(dataSet.containsKey(timestamp)){
						parentSpoutNbOutput += dataSet.get(timestamp);
						dataSet.remove(timestamp);
					}
					dataSet.put(timestamp, parentSpoutNbOutput);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover the number of emitted tuples for bolt " + component + " because " + e);
			}
		}
		HashMap<Integer, Integer> normTimestamps = new HashMap<>();
		ArrayList<Integer> sortedTimestamps = new ArrayList<>();
		sortedTimestamps.addAll(rawTimestamps);
		Collections.sort(sortedTimestamps);
		int nbTimestamp = sortedTimestamps.size();
		int refTimestamp = 0;
		for(int i = 0; i < nbTimestamp; i++){
			normTimestamps.put(sortedTimestamps.get(i), refTimestamp);
			refTimestamp += STEP;
		}
		for(Integer rawTimestamp : rawTimestamps){
			Double value = dataSet.get(rawTimestamp);
			dataSet.remove(rawTimestamp);
			dataSet.put(normTimestamps.get(rawTimestamp), value);
		}
		return dataSet;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> iterations = new HashMap<>();
		HashSet<Integer> timestamps = new HashSet<>();
		HashSet<String> topologyNames = new HashSet<>();
		//Recover the name of each topology in the record collection to merge
		String queryTopologies = "SELECT DISTINCT " + COL_TOPOLOGY +
									" FROM " + TABLE_SPOUT + ";";//spout and bolt tables should contains all topology names but spout table contains generally less records
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(queryTopologies);
			while(result.next()){
				String topology = result.getString(COL_TOPOLOGY);
				topologyNames.add(topology);
			}
		} catch (SQLException e) {
			logger.severe("Unable to extract topologies to merge (network traffic) because " + e);
		}
		//For each topology compute the traffic and add it to the iteration set. Also add recorded timestamps to the timestamp set
		for(String topology : topologyNames){
			HashMap<Integer, Double> iteration = this.getBoltInput(component, topology, structure);
			iterations.put(topology, iteration);
			timestamps.addAll(iteration.keySet());
		}
		//Compute min, average and max series
		Set<String> topologies = iterations.keySet();
		HashMap<Integer, Double> minSerie = new HashMap<>();
		HashMap<Integer, Double> avgSerie = new HashMap<>();
		HashMap<Integer, Double> maxSerie = new HashMap<>();
		
		for(Integer recTimestamp : timestamps){
			ArrayList<Double> values = new ArrayList<>();
			for(String topology : topologies){
				Double value = iterations.get(topology).get(recTimestamp);
				if(value != null){
					values.add(value);
				}
			}
			minSerie.put(recTimestamp, Utils.getMinValue(values));
			avgSerie.put(recTimestamp, Utils.getAvgValue(values));
			maxSerie.put(recTimestamp, Utils.getMaxValue(values));
		}
		alldata.put(this.topology + "_MIN", minSerie);
		alldata.put(this.topology, avgSerie);
		alldata.put(this.topology + "_MAX", maxSerie);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component) {
		return getBoltAggregatedInfo(TABLE_BOLT, component, "SUM(", COL_UPDT_EXEC);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component) {
		return getBoltAggregatedInfo(TABLE_BOLT, component, "SUM(", COL_UPDT_OUTPUT);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component) {
		return getBoltAggregatedInfo(TABLE_BOLT, component, "MAX(", COL_AVG_LATENCY);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component) {
		return null;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltPendings(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltPendings(String component) {
		return getBoltAggregatedInfo(TABLE_ESTIM, component, "MAX(", COL_PENDING);//here it doesn't make a difference with no aggregate
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltActivity(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltActivity(String component) {
		return null;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltCpuUsage(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltCpuUsage(String component) {
		return getBoltAggregatedInfo(TABLE_BOLT, component, "SUM(", COL_CPU);
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltRebalancing(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltRebalancing(String component) {
		return getBoltAggregatedInfo(TABLE_BOLT, component, "COUNT(DISTINCT ", COL_START_TASK);
	}
}