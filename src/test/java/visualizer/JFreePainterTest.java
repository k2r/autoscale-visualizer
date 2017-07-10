/**
 * 
 */
package visualizer;

import java.io.File;
import java.io.FileNotFoundException;

//import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import visualizer.config.LabelParser;
import visualizer.config.XmlConfigParser;
import visualizer.draw.JFreePainter;
import visualizer.source.JdbcSource;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class JFreePainterTest {
	
	public static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/autoscale_test";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		ArrayList<String> queries = new ArrayList<>();
		String queryA1 = "INSERT INTO all_time_spouts_stats VALUES('1', 'host1', '10', 'topologyTest', 'A', '1', '1', '10', '10', '5', '5', '0', '0', '10')";
		String queryB1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host1', '20', 'topologyTest', 'B', '2', '4', '10', '10', '8', '8', '3', '0.8', '20.0')";
		String queryC1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryD1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest', 'D', '8', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryE1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryF1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host3', '10', 'topologyTest', 'F', '14', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryA2 = "INSERT INTO all_time_spouts_stats VALUES('2', 'host1', '10', 'topologyTest', 'A', '1', '1', '20', '10', '10', '5', '5', '5', '22')";
		String queryB21 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host1', '20', 'topologyTest', 'B', '2', '3', '30', '20', '20', '12', '2.5', '0.8', '20.0')";
		String queryB22 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '30', 'topologyTest', 'B', '4', '4', '25', '15', '16', '8', '3.5', '0.8', '20.0')";
		String queryC2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryD2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest', 'D', '8', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryE2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryF2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '10', 'topologyTest', 'F', '14', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryA3 = "INSERT INTO all_time_spouts_stats VALUES('3', 'host1', '10', 'topologyTest', 'A', '1', '1', '35', '15', '12', '2', '6', '1', '8')";
		String queryB3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest', 'B', '2', '4', '35', '5', '40', '4', '4.25', '0.8', '20.0')";
		String queryC3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryD31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '20', 'topologyTest', 'D', '8', '9', '6', '6', '5', '5', '8', '0.84', '30.0')";
		String queryD32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest', 'D', '10', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryE3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '30', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryF31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '10', 'topologyTest', 'F', '14', '15', '5', '5', '0', '0', '20', '0', '40.0')";
		String queryF32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host4', '10', 'topologyTest', 'F', '16', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryStatus1 = "INSERT INTO topologies_status VALUES('1', 'topologyTest', 'ACTIVE')";
		String queryStatus2 = "INSERT INTO topologies_status VALUES('2', 'topologyTest', 'DEACTIVATED')";
		String queryStatus3 = "INSERT INTO topologies_status VALUES('3', 'topologyTest', 'REBALANCING')";
		
		queries.add(queryA1);
		queries.add(queryB1);
		queries.add(queryC1);
		queries.add(queryD1);
		queries.add(queryE1);
		queries.add(queryF1);
		queries.add(queryA2);
		queries.add(queryB21);
		queries.add(queryB22);
		queries.add(queryC2);
		queries.add(queryD2);
		queries.add(queryE2);
		queries.add(queryF2);
		queries.add(queryA3);
		queries.add(queryB3);
		queries.add(queryC3);
		queries.add(queryD31);
		queries.add(queryD32);
		queries.add(queryE3);
		queries.add(queryF31);
		queries.add(queryF32);
		queries.add(queryStatus1);
		queries.add(queryStatus2);
		queries.add(queryStatus3);
		
		for(String query : queries){
			try {
				Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				statement.executeUpdate(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/autoscale_test";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		String cleanQuery1 = "DELETE FROM all_time_spouts_stats";
		String cleanQuery2 = "DELETE FROM all_time_bolts_stats";
		String cleanQuery4 = "DELETE FROM topologies_status";
		
		ArrayList<String> queries = new ArrayList<>();
		queries.add(cleanQuery1);
		queries.add(cleanQuery2);
		queries.add(cleanQuery4);
		
		for(String query : queries){
			try {
				Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				statement.executeUpdate(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		File file1 = new File("topologyTest_linear_increase");
		FileSourceTest.delete(file1);
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyInput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyInput() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyInput();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyThroughput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyThroughput() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyThroughput();;
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyLosses()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyLosses() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyLosses();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyLatency()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyLatency() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyLatency();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyNbExecutors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbExecutors() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbExecutors();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyNbSupervisors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbSupervisors() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbSupervisors();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyNbWorkers()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbWorkers() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbWorkers();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyStatus()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyStatus() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyStatus();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawTopologyTraffic(visualizer.structure.IStructure)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyTraffic() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyTraffic(structure);
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltInput(java.lang.String, visualizer.structure.IStructure)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltInput() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltInput(bolt, structure);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltExecuted(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltExecuted() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltExecuted(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltOutputs(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltOutputs() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltOutputs(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltLatency(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltLatency() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltLatency(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltCpuUsage(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltCpuUsage() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltCpuUsage(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreePainter#drawBoltRebalancing(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testDrawBoltRebalancing() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreePainter painter = new JFreePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltRebalancing(bolt);
		}
	}
}
