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

import visualizer.config.XmlConfigParser;
import visualizer.draw.JFreePainter;
import visualizer.source.FileSource;
import visualizer.source.JdbcSource;
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
		XmlConfigParser parser;
		try {
			parser = new XmlConfigParser("parameters.xml");
			parser.initParameters();
			String command = parser.getCommand();
			if(command.equalsIgnoreCase("ANALYZE")){
				String topology = parser.getTopologies().get(0);
				Integer varCode = parser.getStreamTypes().get(0);
				String dbHost = parser.getDb_host();
				String dbName = parser.getDb_name();
				String dbUser = parser.getDb_user();
				String dbPwd = parser.getDb_pwd();
				
				try {
					JdbcSource jdbcSource = new JdbcSource(dbHost, dbName, dbUser, dbPwd, topology);
					IStructure structure = new TopologyStructure(parser.getEdges());
					JFreePainter painter = new JFreePainter(topology, varCode, jdbcSource);
					
					painter.drawTopologyInput();
					painter.drawTopologyThroughput();
					painter.drawTopologyLosses();
					painter.drawTopologyLatency();
					painter.drawTopologyNbExecutors();
					painter.drawTopologyNbSupervisors();
					painter.drawTopologyNbWorkers();
					//painter.drawTopologyStatus();
					painter.drawTopologyTraffic(structure);
					painter.drawTopologyRebalancing(structure);
					painter.drawTopologyLoad();
					ArrayList<String> bolts = structure.getBolts();
					for(String bolt : bolts){
						painter.drawBoltInput(bolt, structure);
						painter.drawBoltExecuted(bolt);
						painter.drawBoltOutputs(bolt);
						painter.drawBoltLatency(bolt);
						//painter.drawBoltProcRate(bolt);
						painter.drawBoltCR(bolt);
						painter.drawBoltPL(bolt);
					}
					System.out.println("Benchmark extracted and prepared for visualization!");
				} catch (ClassNotFoundException | SQLException e) {
					logger.severe("An error occured while getting connect to the database " + e);
				}
			}
			if(command.equalsIgnoreCase("COMPARE")){
				ArrayList<String> topologies = parser.getTopologies();
				String topologyName = "Comparison of topologies ";
				for(String topology : topologies){
					topologyName += topology + " ";
				}
				ArrayList<Integer> varCodes = parser.getStreamTypes();
				FileSource source = new FileSource(topologies, varCodes);
				IStructure structure = new TopologyStructure(parser.getEdges());
				JFreePainter painter = new JFreePainter(topologyName, varCodes.get(0), source);
				painter.drawTopologyInput();
				painter.drawTopologyThroughput();
				painter.drawTopologyLosses();
				painter.drawTopologyLatency();
				painter.drawTopologyNbExecutors();
				painter.drawTopologyNbSupervisors();
				painter.drawTopologyNbWorkers();
				//painter.drawTopologyStatus();
				painter.drawTopologyTraffic(structure);
				painter.drawTopologyRebalancing(structure);
				painter.drawTopologyLoad();
				ArrayList<String> bolts = structure.getBolts();
				for(String bolt : bolts){
					painter.drawBoltInput(bolt, structure);
					painter.drawBoltExecuted(bolt);
					painter.drawBoltOutputs(bolt);
					painter.drawBoltLatency(bolt);
					//painter.drawBoltProcRate(bolt);
					painter.drawBoltCR(bolt);
					painter.drawBoltPL(bolt);
				}
				System.out.println("Benchmarks comparison prepared for visualization!");
			}
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.severe("An error occured while initializing the benchmark visualizer " + e);
		}
	}
}