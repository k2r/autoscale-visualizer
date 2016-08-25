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
				String topology = parser.getTopology1();
				Integer varCode = parser.getStreamType1();
				String dbHost = parser.getDb_host();
				String dbName = parser.getDb_name();
				String dbUser = parser.getDb_user();
				String dbPwd = parser.getDb_pwd();
				
				try {
					JdbcSource jdbcSource = new JdbcSource(dbHost, dbName, dbUser, dbPwd, topology);
					IStructure structure = new TopologyStructure(parser.getEdges1());
					JFreePainter painter = new JFreePainter(topology, varCode, jdbcSource);
					
					painter.drawTopologyInput();
					painter.drawTopologyThroughput();
					painter.drawTopologyLosses();
					painter.drawTopologyLatency();
					painter.drawTopologyNbExecutors();
					painter.drawTopologyNbSupervisors();
					painter.drawTopologyNbWorkers();
					painter.drawTopologyStatus();
					painter.drawTopologyTraffic(structure);
					ArrayList<String> bolts1 = structure.getBolts();
					for(String bolt : bolts1){
						painter.drawBoltInput(bolt, structure);
						painter.drawBoltExecuted(bolt);
						painter.drawBoltOutputs(bolt);
						painter.drawBoltLatency(bolt);
						painter.drawBoltProcRate(bolt);
						painter.drawBoltEPR(bolt);
					}
					System.out.println("Benchmark extracted and prepared for visualization!");
				} catch (ClassNotFoundException | SQLException e) {
					logger.severe("An error occured while getting connect to the database " + e);
				}
			}
			if(command.equalsIgnoreCase("COMPARE")){
				String topology1 = parser.getTopology1();
				String topology2 = parser.getTopology2();
				String topology = topology1 + "_vs_" + topology2;
				Integer varCode1 = parser.getStreamType1();
				Integer varCode2 = parser.getStreamType2();
				FileSource source = new FileSource(topology1, varCode1, topology2, varCode2);
				IStructure structure = new TopologyStructure(parser.getEdges1());
				JFreePainter painter = new JFreePainter(topology, varCode1, source);
				painter.drawTopologyInput();
				painter.drawTopologyThroughput();
				painter.drawTopologyLosses();
				painter.drawTopologyLatency();
				painter.drawTopologyNbExecutors();
				painter.drawTopologyNbSupervisors();
				painter.drawTopologyNbWorkers();
				painter.drawTopologyStatus();
				painter.drawTopologyTraffic(structure);
				ArrayList<String> bolts1 = structure.getBolts();
				for(String bolt : bolts1){
					painter.drawBoltInput(bolt, structure);
					painter.drawBoltExecuted(bolt);
					painter.drawBoltOutputs(bolt);
					painter.drawBoltLatency(bolt);
					painter.drawBoltProcRate(bolt);
					painter.drawBoltEPR(bolt);
				}
				System.out.println("Benchmarks comparison prepared for visualization!");
			}
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.severe("An error occured while initializing the benchmark visualizer " + e);
		}
	}
}