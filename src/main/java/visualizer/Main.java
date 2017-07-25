/**
 * 
 */
package visualizer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import visualizer.config.LabelParser;
import visualizer.config.XmlConfigParser;
import visualizer.draw.JFreeMergePainter;
import visualizer.draw.JFreePainter;
import visualizer.source.FileSource;
import visualizer.source.JdbcMergeSource;
import visualizer.source.JdbcSource;
import visualizer.source.FileMergeSource;
import visualizer.structure.IStructure;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class Main {

	private static Logger logger = Logger.getLogger("Benchmark-visualizer");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		XmlConfigParser configParser;
		LabelParser labelParser;
		try {
			configParser = new XmlConfigParser("parameters.xml");
			configParser.initParameters();
			labelParser = new LabelParser(configParser.getLanguage());
			labelParser.initParameters();
			String command = configParser.getCommand();
			if(command.equalsIgnoreCase("ANALYZE")){
				String topology = configParser.getTopologies().get(0);
				Integer varCode = configParser.getStreamTypes().get(0);
				String dbHost = configParser.getDb_host();
				String dbName = configParser.getDb_name();
				String dbUser = configParser.getDb_user();
				String dbPwd = configParser.getDb_pwd();

				try {
					JdbcSource jdbcSource = new JdbcSource(dbHost, dbName, dbUser, dbPwd, topology);
					IStructure structure = new TopologyStructure(configParser.getEdges());
					JFreePainter painter = new JFreePainter(topology, varCode, jdbcSource, configParser, labelParser);

					painter.drawTopologyInput();
					painter.drawTopologyThroughput();
					painter.drawTopologyLosses();
					painter.drawTopologyLatency();
					painter.drawTopologyNbExecutors();
					painter.drawTopologyNbSupervisors();
					painter.drawTopologyNbWorkers();
					painter.drawTopologyTraffic(structure);
					//painter.drawTopologyRebalancing(structure); deprecated
					ArrayList<String> bolts = structure.getBolts();
					for(String bolt : bolts){
						painter.drawBoltInput(bolt, structure);
						painter.drawBoltExecuted(bolt);
						painter.drawBoltOutputs(bolt);
						painter.drawBoltLatency(bolt);
						painter.drawBoltCpuUsage(bolt);
						painter.drawBoltRebalancing(bolt);
						painter.drawBoltPending(bolt);
					}
					System.out.println("Benchmark extracted and prepared for visualization!");
				} catch (ClassNotFoundException | SQLException e) {
					logger.severe("An error occured while getting connect to the database " + e);
				}
			}
			if(command.equalsIgnoreCase("COMPARE")){
				ArrayList<String> topologies = configParser.getTopologies();
				String topologyName = "Comparison of topologies";
				for(String topology : topologies){
					String shortName = configParser.getShortTopName(topology);
					topologyName += " " + shortName;
				}
				ArrayList<Integer> varCodes = configParser.getStreamTypes();
				FileSource source = new FileSource(topologies, varCodes);
				IStructure structure = new TopologyStructure(configParser.getEdges());
				JFreePainter painter = new JFreePainter(topologyName, varCodes.get(0), source, configParser, labelParser);
				painter.drawTopologyInput();
				painter.drawTopologyThroughput();
				painter.drawTopologyLosses();
				painter.drawTopologyLatency();
				painter.drawTopologyNbExecutors();
				painter.drawTopologyNbSupervisors();
				painter.drawTopologyNbWorkers();
				painter.drawTopologyTraffic(structure);
				//painter.drawTopologyRebalancing(structure); deprecated
				ArrayList<String> bolts = structure.getBolts();
				for(String bolt : bolts){
					painter.drawBoltInput(bolt, structure);
					painter.drawBoltExecuted(bolt);
					painter.drawBoltOutputs(bolt);
					painter.drawBoltLatency(bolt);
					painter.drawBoltCpuUsage(bolt);
					painter.drawBoltRebalancing(bolt);
					painter.drawBoltPending(bolt);
				}
				System.out.println("Benchmarks comparison prepared for visualization!");
			}
			if(command.equalsIgnoreCase("MERGE")){
				String topology = configParser.getTopologies().get(0);
				Integer varCode = configParser.getStreamTypes().get(0);
				String dbHost = configParser.getDb_host();
				String dbName = configParser.getDb_name();
				String dbUser = configParser.getDb_user();
				String dbPwd = configParser.getDb_pwd();
				try {
					JdbcMergeSource jdbcMergeSource = new JdbcMergeSource(dbHost, dbName, dbUser, dbPwd, topology);
					IStructure structure = new TopologyStructure(configParser.getEdges());
					JFreeMergePainter painter = new JFreeMergePainter(topology, varCode, jdbcMergeSource, configParser, labelParser);

					painter.drawTopologyInput();
					painter.drawTopologyThroughput();
					painter.drawTopologyLosses();
					painter.drawTopologyLatency();
					painter.drawTopologyNbExecutors();
					painter.drawTopologyNbSupervisors();
					painter.drawTopologyNbWorkers();
					painter.drawTopologyTraffic(structure);
					//painter.drawTopologyRebalancing(structure); deprecated
					ArrayList<String> bolts = structure.getBolts();
					for(String bolt : bolts){
						painter.drawBoltInput(bolt, structure);
						painter.drawBoltExecuted(bolt);
						painter.drawBoltOutputs(bolt);
						painter.drawBoltLatency(bolt);
						painter.drawBoltCpuUsage(bolt);
						painter.drawBoltRebalancing(bolt);
						painter.drawBoltPending(bolt);
					}
					System.out.println("Benchmark extracted and prepared for visualization!");
				} catch (ClassNotFoundException | SQLException e) {
					logger.severe("An error occured while getting connect to the database " + e);
				}
			}
			if(command.equalsIgnoreCase("COMPARE-MERGED")){
				ArrayList<String> topologies = configParser.getTopologies();
				String topologyName = "Comparison of topologies";
				for(String topology : topologies){
					String shortName = configParser.getShortTopName(topology);
					topologyName += " " + shortName;
				}
				ArrayList<Integer> varCodes = configParser.getStreamTypes();
				FileMergeSource fileMergeSource = new FileMergeSource(topologies, varCodes);
				IStructure structure = new TopologyStructure(configParser.getEdges());
				JFreeMergePainter painter = new JFreeMergePainter(topologyName, varCodes.get(0), fileMergeSource, configParser, labelParser);
				painter.drawTopologyInput();
				painter.drawTopologyThroughput();
				painter.drawTopologyLosses();
				painter.drawTopologyLatency();
				painter.drawTopologyNbExecutors();
				painter.drawTopologyNbSupervisors();
				painter.drawTopologyNbWorkers();
				painter.drawTopologyTraffic(structure);
				//painter.drawTopologyRebalancing(structure); deprecated
				ArrayList<String> bolts = structure.getBolts();
				for(String bolt : bolts){
					painter.drawBoltInput(bolt, structure);
					painter.drawBoltExecuted(bolt);
					painter.drawBoltOutputs(bolt);
					painter.drawBoltLatency(bolt);
					painter.drawBoltCpuUsage(bolt);
					painter.drawBoltRebalancing(bolt);
					painter.drawBoltPending(bolt);
				}
				System.out.println("Benchmarks comparison prepared for visualization!");
			}
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.severe("An error occured while initializing the benchmark visualizer " + e);
		}
	}
}