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
//import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public class JdbcSource implements ISource {

	private String topology;
	private final Connection connection;
	private int referenceTimestamp;
	
	private final static String TABLE_SPOUT = "all_time_spouts_stats";
	private final static String TABLE_BOLT = "all_time_bolts_stats";
	private final static String TABLE_STATUS = "topologies_status";
	private final static String TABLE_ACTIVITY = "operators_activity";
	private final static String TABLE_LOADS = "operators_loads";
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
	private final static String COL_ACTIVTY = "activity_level";
	private final static String COL_PROCRATE = "processing_ratio";
	private final static String COL_LOAD = "current_load";
	private final static String COL_CAPACITY = "capacity_per_second";
	private final static String COL_STATUS = "status";
	//private final static String COL_CURRENT = "current_parallelism";
	//private final static String COL_NEW = "new_parallelism";
	
	
	private static final Logger logger = Logger.getLogger("JdbcSource");
	
	public JdbcSource(String dbHost, String dbName, String dbUser, String dbPwd, String topology) throws SQLException, ClassNotFoundException {
		this.topology = topology;
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/" + dbName;
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
		String query = "SELECT " + COL_TIMESTAMP + " FROM " + TABLE_SPOUT;
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			this.referenceTimestamp = 0;
			if(result.first()){
				this.referenceTimestamp = result.getInt(COL_TIMESTAMP) - 1;
			}
		}catch (SQLException e) {
			logger.severe("Unable to recover the reference timestamp because " + e);
		}
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyInput() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double topInput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
				dataSet.put(timestamp, topInput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology input because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyThroughput() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_THROUGHPUT + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double topThroughput = result.getDouble("SUM(" + COL_UPDT_THROUGHPUT + ")");
				dataSet.put(timestamp, topThroughput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology throughput because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLosses(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyDephase() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_LOSS + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double topLoss = result.getDouble("SUM(" + COL_UPDT_LOSS + ")");
				dataSet.put(timestamp, topLoss);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology losses because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLatency() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", MAX(" + COL_AVG_COMPLETE_LATENCY + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double topLatency = result.getDouble("MAX(" + COL_AVG_COMPLETE_LATENCY + ")");
				dataSet.put(timestamp, topLatency);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology average complete latency because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbExecutors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbExecutors() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String querySpout = "SELECT " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statementSpout;
		try {
			statementSpout = this.connection.createStatement();
			ResultSet resultSpout = statementSpout.executeQuery(querySpout);
			while(resultSpout.next()){
				Integer timestamp = resultSpout.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double nbExecutors = resultSpout.getDouble("COUNT(DISTINCT " + COL_START_TASK + ")");
				dataSet.put(timestamp, nbExecutors);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of executors for the topology because " + e);
		}
		String queryBolt = "SELECT " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") " +
				" FROM " + TABLE_BOLT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statementBolt;
		try {
			statementBolt = this.connection.createStatement();
			ResultSet resultBolt = statementBolt.executeQuery(queryBolt);
			while(resultBolt.next()){
				Integer timestamp = resultBolt.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double nbExecutors = resultBolt.getDouble("COUNT(DISTINCT " + COL_START_TASK + ")");
				if(dataSet.containsKey(timestamp)){
					nbExecutors += dataSet.get(timestamp);
					dataSet.remove(timestamp);
				}
				dataSet.put(timestamp, nbExecutors);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of executors for the topology because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbSupervisors() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT C." + COL_TIMESTAMP + ", COUNT(DISTINCT C." + COL_HOST + ") " +
				" FROM(" +
				" SELECT " + COL_TIMESTAMP + ", " + COL_HOST +
				" FROM " + TABLE_SPOUT + 
				" UNION " + 
				" SELECT " + COL_TIMESTAMP + ", " + COL_HOST +
				" FROM " + TABLE_BOLT +  
				") C" + 
				" GROUP BY C." + COL_TIMESTAMP;
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double nbSupervisors = result.getDouble("COUNT(DISTINCT C." + COL_HOST + ")");
				dataSet.put(timestamp, nbSupervisors);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of supervisors for the topology because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyNbWorkers() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT D." + COL_TIMESTAMP + ", SUM(D.nbport) AS nbWorkers " +
				" FROM(" +
				" SELECT C." + COL_TIMESTAMP + ", COUNT(DISTINCT C." + COL_PORT + ") AS nbport" +
				" FROM(" +
				" SELECT " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT +
				" FROM " + TABLE_SPOUT + 
				" UNION " + 
				" SELECT " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT +
				" FROM " + TABLE_BOLT +  
				") C" + 
				" GROUP BY C." + COL_TIMESTAMP + ", C." + COL_HOST + ") D" +
				" GROUP BY D." + COL_TIMESTAMP;
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double nbWorkers = result.getDouble("nbWorkers");
				dataSet.put(timestamp, nbWorkers);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of workers for the topology because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyStatus() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_STATUS  +
				" FROM " + TABLE_STATUS + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				String topStatus = result.getString(COL_STATUS);
				Double code = -2.0;
				if(topStatus.equalsIgnoreCase("ACTIVE")){
					code = 2.0;
				}
				if(topStatus.equalsIgnoreCase("REBALANCING")){
					code = 1.0;
				}
				if(topStatus.equalsIgnoreCase("DEACTIVATED")){
					code = 0.0;
				}
				if(topStatus.equalsIgnoreCase("KILLED")){
					code = -1.0;
				}
				dataSet.put(timestamp, code);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology status because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyTraffic(IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		ArrayList<String> bolts = structure.getBolts();
		for(String bolt : bolts){
			String queryBolt = "SELECT " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
					" FROM " + TABLE_BOLT +
					" WHERE " + COL_COMPONENT + " = '" + bolt + "' " +
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
			Statement statementBolt;
			try {
				statementBolt = this.connection.createStatement();
				ResultSet resultBolt = statementBolt.executeQuery(queryBolt);
				HashMap<Integer, HashMap<String, Double>> boltOutputPerHost = new HashMap<>();
				while(resultBolt.next()){
					Integer timestamp = resultBolt.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
					String queryChild = "SELECT " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
							" FROM " + TABLE_BOLT +
							" WHERE " + COL_COMPONENT + " = '" + child + "' " +
							" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
					Statement statementChild;
					try {
						statementChild = this.connection.createStatement();
						ResultSet resultChild = statementChild.executeQuery(queryChild);
						HashMap<Integer, HashMap<String, Double>> childOutputPerHost = new HashMap<>();
						while(resultChild.next()){
							Integer timestamp = resultChild.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
						for(Integer timestamp : boltOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = boltOutputPerHost.get(timestamp);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(timestamp);
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
							if(!dataSet.containsKey(timestamp)){
								dataSet.put(timestamp, 0.0);
							}
							Double globalTrafic = dataSet.get(timestamp);
							globalTrafic += traffic;
							dataSet.remove(timestamp);
							dataSet.put(timestamp, globalTrafic);
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
			String querySpout = "SELECT " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
					" FROM " + TABLE_SPOUT +
					" WHERE " + COL_COMPONENT + " = '" + spout + "'" + 
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
			Statement statementSpout;
			try {
				statementSpout = this.connection.createStatement();
				ResultSet resultSpout = statementSpout.executeQuery(querySpout);
				HashMap<Integer, HashMap<String, Double>> spoutOutputPerHost = new HashMap<>();
				while(resultSpout.next()){
					Integer timestamp = resultSpout.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
					String queryChild = "SELECT " + COL_TIMESTAMP + ", " + COL_HOST  + ", SUM(" + COL_UPDT_OUTPUT + ")" +
							" FROM " + TABLE_BOLT +
							" WHERE " + COL_COMPONENT + " = '" + child + "'" + 
							" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST; 
					Statement statementChild;
					try {
						statementChild = this.connection.createStatement();
						ResultSet resultChild = statementChild.executeQuery(queryChild);
						HashMap<Integer, HashMap<String, Double>> childOutputPerHost = new HashMap<>();
						while(resultChild.next()){
							Integer timestamp = resultChild.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
						for(Integer timestamp : spoutOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = spoutOutputPerHost.get(timestamp);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(timestamp);
							Set<String> boltHosts = bolthostOutput.keySet();
							if(childHostOuput != null){
								for(String host : boltHosts){
									if(!childHostOuput.containsKey(host)){
										traffic += bolthostOutput.get(host);
									}
								}
							}
							if(!dataSet.containsKey(timestamp)){
								dataSet.put(timestamp, 0.0);
							}
							Double globalTrafic = dataSet.get(timestamp);
							globalTrafic += traffic;
							dataSet.remove(timestamp);
							dataSet.put(timestamp, globalTrafic);
						}
					}catch (SQLException e) {
						logger.severe("Unable to recover topology traffic because " + e);
					}
				}
			}catch (SQLException e) {
				logger.severe("Unable to recover topology traffic because " + e);
			}
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltInput(String component, IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		ArrayList<String> parents = structure.getParents(component);
		for(String parent : parents){
			String queryBolt = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
					" FROM " + TABLE_BOLT + 
					" WHERE " + COL_COMPONENT + " = '" + parent + "'" +
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
			Statement statementBolt;
			try {
				statementBolt = this.connection.createStatement();
				ResultSet result = statementBolt.executeQuery(queryBolt);
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
			
			String querySpout = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
					" FROM " + TABLE_SPOUT + 
					" WHERE " + COL_COMPONENT + " = '" + parent + "'" +
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
			Statement statementSpout;
			try {
				statementSpout = this.connection.createStatement();
				ResultSet result = statementSpout.executeQuery(querySpout);
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
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
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltExecuted(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_EXEC + ") " +
				" FROM " + TABLE_BOLT + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'" +
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltNbExecuted = result.getDouble("SUM(" + COL_UPDT_EXEC + ")");
				dataSet.put(timestamp, boltNbExecuted);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of executed tuples for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltOutputs(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
				" FROM " + TABLE_BOLT + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'" +
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltNbOutput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
				dataSet.put(timestamp, boltNbOutput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of emitted tuples for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltLatency(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", MAX(" + COL_AVG_LATENCY + ") " +
				" FROM " + TABLE_BOLT + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'" +
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltAvgLatency = result.getDouble("MAX(" + COL_AVG_LATENCY + ")");
				dataSet.put(timestamp, boltAvgLatency);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the average latency per tuple for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltProcessingRate(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_CAPACITY +
				" FROM " + TABLE_ACTIVITY + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'";
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltProcRate = result.getDouble(COL_CAPACITY);
				dataSet.put(timestamp, boltProcRate);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the processing rate for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltCR(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltCR(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_ACTIVTY +
				" FROM " + TABLE_ACTIVITY + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'"; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltCR = result.getDouble(COL_ACTIVTY);
				dataSet.put(timestamp, boltCR);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the congestion risk value for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltPL(java.lang.String)
	 */
	@Override
	public HashMap<String, HashMap<Integer, Double>> getBoltPL(String component) {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_PROCRATE +
				" FROM " + TABLE_LOADS + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'"; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double boltPL = result.getDouble(COL_PROCRATE);
				dataSet.put(timestamp, boltPL);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the processing load value for bolt " + component + " because " + e);
		}
		alldata.put(this.topology, dataSet);
		return alldata;
	}

	@Override
	public HashMap<String, HashMap<String, HashMap<Integer, Double>>> getTopologyRebalancing(IStructure structure) {
		HashMap<String, HashMap<String, HashMap<Integer,Double>>> alldata = new HashMap<>();
		HashMap<String, HashMap<Integer, Double>> dataSet = new HashMap<>();
		
		ArrayList<String> bolts = structure.getBolts();
		for(String bolt : bolts){
			String queryExecutors = "SELECT " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") AS nbExecutors" +
					" FROM " + TABLE_BOLT +
					" WHERE " + COL_COMPONENT + " = '" + bolt + "' " + 
					" GROUP BY " + COL_TIMESTAMP;
			Statement statementExecutors;
			try{
				statementExecutors = this.connection.createStatement();
				ResultSet result = statementExecutors.executeQuery(queryExecutors);
				HashMap<Integer, Double> componentInfo = dataSet.get(bolt);
				if(componentInfo == null){
					componentInfo = new HashMap<>();
				}
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
					Integer nbExecutors = result.getInt("nbExecutors");
					componentInfo.put(timestamp, nbExecutors.doubleValue());
				}
				dataSet.put(bolt, componentInfo);
			}catch (SQLException e) {
				logger.severe("Unable to recover scaling actions for the topology because " + e);
			}
		}
		
		/*HashMap<Integer, HashMap<String, Double>> rebalancing = new HashMap<>();
		String queryRebalancing = "SELECT " + COL_TIMESTAMP + ", " + COL_COMPONENT + ", SUM(" + COL_CURRENT + "), SUM(" + COL_NEW + ") " + 
				" FROM " + TABLE_SCALE + 
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_COMPONENT; 
		Statement statementRebalancing;
		try {
			statementRebalancing = this.connection.createStatement();
			ResultSet result = statementRebalancing.executeQuery(queryRebalancing);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				String component = result.getString(COL_COMPONENT);
				Integer currentP = result.getInt("SUM(" + COL_CURRENT + ")");
				Integer newP = result.getInt("SUM(" + COL_NEW + ")");
				Double rebalance =  1.0 * newP - currentP;
				HashMap<String, Double> timestampInfo = rebalancing.get(timestamp);
				if(timestampInfo == null){
					timestampInfo = new HashMap<>();
				}
				timestampInfo.put(component, rebalance);
				rebalancing.put(timestamp, timestampInfo);
			}
			for(Integer timestamp : rebalancing.keySet()){
				HashMap<String, Double> timestampInfo = rebalancing.get(timestamp);
				for(String bolt : bolts){
					if(!timestampInfo.containsKey(bolt)){
						timestampInfo.put(bolt, 0.0);
					}
				}
				rebalancing.put(timestamp, timestampInfo);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover scaling actions for the topology because " + e);
		}
		
		HashMap<Integer, HashMap<String, Double>> dataSet = new HashMap<>();
		ArrayList<Integer> orderedTimestamp = new ArrayList<>();
		for(Integer timestamp : nbExecutorsPerComponent.keySet()){
			orderedTimestamp.add(timestamp);
		}
		Collections.sort(orderedTimestamp);
		Integer init = orderedTimestamp.get(0); 
		dataSet.put(init, nbExecutorsPerComponent.get(init));
		for(Integer timestamp : rebalancing.keySet()){
			if(orderedTimestamp.contains(timestamp)){
				Integer index = orderedTimestamp.indexOf(timestamp);
				Integer effectiveIndex = Math.min(index + 2, nbExecutorsPerComponent.size() - 1);
				HashMap<String, Double> relevantExecutors = nbExecutorsPerComponent.get(orderedTimestamp.get(effectiveIndex));
				dataSet.put(timestamp, relevantExecutors);
			}
		}*/
 		alldata.put(this.topology, dataSet);
		return alldata;
	}

	@Override
	public HashMap<String, HashMap<Integer, Double>> getTopologyLoads() {
		HashMap<String, HashMap<Integer, Double>> alldata = new HashMap<>();
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", AVG(" + COL_LOAD + ") AS avgLoad" +
				" FROM " + TABLE_LOADS + 
				" GROUP BY " + COL_TIMESTAMP + "," + COL_TOPOLOGY;
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP) - this.referenceTimestamp;
				Double avgLoad = result.getDouble("avgLoad");
				dataSet.put(timestamp, avgLoad);
			}
			alldata.put(this.topology, dataSet);
		}catch(SQLException e) {
			logger.severe("Unable to recover global load for the topology because " + e);
		}
		return alldata;
	}
}