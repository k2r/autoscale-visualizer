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
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public class JdbcSource implements ISource {

	private final Connection connection;
	
	private final static String TABLE_SPOUT = "all_time_spouts_stats";
	private final static String TABLE_BOLT = "all_time_bolts_stats";
	private final static String TABLE_STATUS = "topologies_status";
	private final static String TABLE_EPR = "operators_epr";
	
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
	private final static String COL_EPR = "epr";
	private final static String COL_PROC_RATE = "processing_rate";
	private final static String COL_STATUS = "status";
	
	private static final Logger logger = Logger.getLogger("JdbcSource");
	
	public JdbcSource(String dbHost, String dbName, String dbUser, String dbPwd) throws SQLException, ClassNotFoundException {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/" + dbName;
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
	}
	
	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyInput()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyInput() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double topInput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
				dataSet.put(timestamp, topInput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology input because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyThroughput()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyThroughput() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_THROUGHPUT + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double topThroughput = result.getDouble("SUM(" + COL_UPDT_THROUGHPUT + ")");
				dataSet.put(timestamp, topThroughput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology throughput because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLosses(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<Integer, Double> getTopologyLosses() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_LOSS + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double topLoss = result.getDouble("SUM(" + COL_UPDT_LOSS + ")");
				dataSet.put(timestamp, topLoss);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology losses because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyLatency()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyLatency() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", MAX(" + COL_AVG_COMPLETE_LATENCY + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double topLatency = result.getDouble("MAX(" + COL_AVG_COMPLETE_LATENCY + ")");
				dataSet.put(timestamp, topLatency);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover topology average complete latency because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbExecutors()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyNbExecutors() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String querySpout = "SELECT " + COL_TIMESTAMP + ", COUNT(DISTINCT " + COL_START_TASK + ") " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statementSpout;
		try {
			statementSpout = this.connection.createStatement();
			ResultSet resultSpout = statementSpout.executeQuery(querySpout);
			while(resultSpout.next()){
				Integer timestamp = resultSpout.getInt(COL_TIMESTAMP);
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
				Integer timestamp = resultBolt.getInt(COL_TIMESTAMP);
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
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbSupervisors()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyNbSupervisors() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT C." + COL_TIMESTAMP + ", COUNT(DISTINCT C." + COL_HOST + ") " +
				" FROM (SELECT * FROM " + TABLE_SPOUT + " JOIN " + TABLE_BOLT + " ON " + COL_TIMESTAMP + ") AS COMPONENT C" +
				" GROUP BY C." + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double nbSupervisors = result.getDouble("COUNT(DISTINCT " + COL_HOST + ")");
				dataSet.put(timestamp, nbSupervisors);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of supervisors for the topology because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyNbWorkers()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyNbWorkers() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String querySpout = "SELECT " + COL_TIMESTAMP + ", COUNT(*) " +
				" FROM " + TABLE_SPOUT + 
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT; 
		Statement statementSpout;
		try {
			statementSpout = this.connection.createStatement();
			ResultSet resultSpout = statementSpout.executeQuery(querySpout);
			while(resultSpout.next()){
				Integer timestamp = resultSpout.getInt(COL_TIMESTAMP);
				Double nbWorkers = resultSpout.getDouble("COUNT(*)");
				dataSet.put(timestamp, nbWorkers);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of workers for the topology because " + e);
		}
		String queryBolt = "SELECT " + COL_TIMESTAMP + ", COUNT(*) " +
				" FROM " + TABLE_BOLT + 
				" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT; 
		Statement statementBolt;
		try {
			statementBolt = this.connection.createStatement();
			ResultSet resultBolt = statementBolt.executeQuery(queryBolt);
			while(resultBolt.next()){
				Integer timestamp = resultBolt.getInt(COL_TIMESTAMP);
				Double nbWorkers = resultBolt.getDouble("COUNT(*)");
				if(dataSet.containsKey(timestamp)){
					nbWorkers += dataSet.get(timestamp);
					dataSet.remove(timestamp);
				}
				dataSet.put(timestamp, nbWorkers);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of workers for the topology because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyStatus()
	 */
	@Override
	public HashMap<Integer, Double> getTopologyStatus() {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_STATUS  +
				" FROM " + TABLE_STATUS + 
				" GROUP BY " + COL_TIMESTAMP; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
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
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<Integer, Double> getTopologyTraffic(IStructure structure) {
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
						for(Integer timestamp : boltOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = boltOutputPerHost.get(timestamp);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(timestamp);
							Set<String> boltHosts = bolthostOutput.keySet();
							for(String host : boltHosts){
								if(!childHostOuput.containsKey(host)){
									traffic += bolthostOutput.get(host);
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
						for(Integer timestamp : spoutOutputPerHost.keySet()){
							Double traffic = 0.0;
							HashMap<String, Double> bolthostOutput = spoutOutputPerHost.get(timestamp);
							HashMap<String, Double> childHostOuput = childOutputPerHost.get(timestamp);
							Set<String> boltHosts = bolthostOutput.keySet();
							for(String host : boltHosts){
								if(!childHostOuput.containsKey(host)){
									traffic += bolthostOutput.get(host);
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
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public HashMap<Integer, Double> getBoltInput(String component, IStructure structure) {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		ArrayList<String> parents = structure.getParents(component);
		for(String parent : parents){
			String query = "SELECT " + COL_TIMESTAMP + ", SUM(" + COL_UPDT_OUTPUT + ") " +
					" FROM " + TABLE_BOLT + 
					" WHERE " + COL_COMPONENT + " = '" + parent + "'" +
					" GROUP BY " + COL_TIMESTAMP + ", " + COL_HOST + ", " + COL_PORT + ", " + COL_TOPOLOGY + ", " + COL_COMPONENT ; 
			Statement statement;
			try {
				statement = this.connection.createStatement();
				ResultSet result = statement.executeQuery(query);
				while(result.next()){
					Integer timestamp = result.getInt(COL_TIMESTAMP);
					Double parentNbOutput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
					if(dataSet.containsKey(timestamp)){
						parentNbOutput += dataSet.get(timestamp);
						dataSet.remove(timestamp);
					}
					dataSet.put(timestamp, parentNbOutput);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover the number of emitted tuples for bolt " + component + " because " + e);
			}
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltExecuted(java.lang.String)
	 */
	@Override
	public HashMap<Integer, Double> getBoltExecuted(String component) {
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
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double boltNbExecuted = result.getDouble("SUM(" + COL_UPDT_EXEC + ")");
				dataSet.put(timestamp, boltNbExecuted);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of executed tuples for bolt " + component + " because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltOutputs(java.lang.String)
	 */
	@Override
	public HashMap<Integer, Double> getBoltOutputs(String component) {
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
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double boltNbOutput = result.getDouble("SUM(" + COL_UPDT_OUTPUT + ")");
				dataSet.put(timestamp, boltNbOutput);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the number of emitted tuples for bolt " + component + " because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltLatency(java.lang.String)
	 */
	@Override
	public HashMap<Integer, Double> getBoltLatency(String component) {
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
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double boltAvgLatency = result.getDouble("MAX(" + COL_AVG_LATENCY + ")");
				dataSet.put(timestamp, boltAvgLatency);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the average latency per tuple for bolt " + component + " because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltProcessingRate(java.lang.String)
	 */
	@Override
	public HashMap<Integer, Double> getBoltProcessingRate(String component) {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_PROC_RATE +
				" FROM " + TABLE_EPR + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'";
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double boltProcRate = result.getDouble(COL_PROC_RATE);
				dataSet.put(timestamp, boltProcRate);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the processing rate for bolt " + component + " because " + e);
		}
		return dataSet;
	}

	/* (non-Javadoc)
	 * @see visualizer.source.ISource#getBoltEPR(java.lang.String)
	 */
	@Override
	public HashMap<Integer, Double> getBoltEPR(String component) {
		HashMap<Integer, Double> dataSet = new HashMap<>();
		String query = "SELECT " + COL_TIMESTAMP + ", " + COL_EPR +
				" FROM " + TABLE_EPR + 
				" WHERE " + COL_COMPONENT + " = '" + component + "'"; 
		Statement statement;
		try {
			statement = this.connection.createStatement();
			ResultSet result = statement.executeQuery(query);
			while(result.next()){
				Integer timestamp = result.getInt(COL_TIMESTAMP);
				Double boltEPR = result.getDouble(COL_EPR);
				dataSet.put(timestamp, boltEPR);
			}
		} catch (SQLException e) {
			logger.severe("Unable to recover the epr value for bolt " + component + " because " + e);
		}
		return dataSet;
	}

}