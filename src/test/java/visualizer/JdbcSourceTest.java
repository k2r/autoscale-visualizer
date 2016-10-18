/**
 * 
 */
package visualizer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import visualizer.config.XmlConfigParser;
import visualizer.source.JdbcSource;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class JdbcSourceTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/benchmarks";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		ArrayList<String> queries = new ArrayList<>();
		String queryA1 = "INSERT INTO all_time_spouts_stats VALUES('1', 'host1', '10', 'topologyTest', 'A', '1', '1', '10', '10', '5', '5', '0', '0', '10')";
		String queryB1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host1', '20', 'topologyTest', 'B', '2', '4', '10', '10', '8', '8', '3', '0.8')";
		String queryC1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1')";
		String queryD1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest', 'D', '8', '10', '6', '6', '0', '0', '10', '0')";
		String queryE1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84')";
		String queryF1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host3', '10', 'topologyTest', 'F', '14', '16', '5', '5', '0', '0', '20', '0')";
		
		String queryA2 = "INSERT INTO all_time_spouts_stats VALUES('2', 'host1', '10', 'topologyTest', 'A', '1', '1', '20', '10', '10', '5', '5', '5', '22')";
		String queryB21 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host1', '20', 'topologyTest', 'B', '2', '3', '30', '20', '20', '12', '2.5', '0.8')";
		String queryB22 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '30', 'topologyTest', 'B', '4', '4', '25', '15', '16', '8', '3.5', '0.8')";
		String queryC2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1')";
		String queryD2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest', 'D', '8', '10', '6', '6', '0', '0', '10', '0')";
		String queryE2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84')";
		String queryF2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '10', 'topologyTest', 'F', '14', '16', '5', '5', '0', '0', '20', '0')";
		
		String queryA3 = "INSERT INTO all_time_spouts_stats VALUES('3', 'host1', '10', 'topologyTest', 'A', '1', '1', '35', '15', '12', '2', '6', '1', '8')";
		String queryB3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest', 'B', '2', '4', '35', '5', '40', '4', '4.25', '0.8')";
		String queryC3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '10', 'topologyTest', 'C', '5', '7', '8', '8', '6', '6', '5', '1')";
		String queryD31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '20', 'topologyTest', 'D', '8', '9', '6', '6', '5', '5', '8', '0.84')";
		String queryD32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest', 'D', '10', '10', '6', '6', '0', '0', '10', '0')";
		String queryE3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '30', 'topologyTest', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84')";
		String queryF31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '10', 'topologyTest', 'F', '14', '15', '5', '5', '0', '0', '20', '0')";
		String queryF32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host4', '10', 'topologyTest', 'F', '16', '16', '5', '5', '0', '0', '20', '0')";
		
		String queryStatus1 = "INSERT INTO topologies_status VALUES('1', 'topologyTest', 'ACTIVE')";
		String queryStatus2 = "INSERT INTO topologies_status VALUES('2', 'topologyTest', 'DEACTIVATED')";
		String queryStatus3 = "INSERT INTO topologies_status VALUES('3', 'topologyTest', 'REBALANCING')";
		
		String crB1 = "INSERT INTO operators_cr VALUES('1', 'topologyTest', 'B', '0.8', '0', '3330')";
		String crC1 = "INSERT INTO operators_cr VALUES('1', 'topologyTest', 'C', '0.75', '0', '2000')";
		String crD1 = "INSERT INTO operators_cr VALUES('1', 'topologyTest', 'D', '1.2', '0', '1250')";
		String crE1 = "INSERT INTO operators_cr VALUES('1', 'topologyTest', 'E', '0.8', '0', '500')";
		String crF1 = "INSERT INTO operators_cr VALUES('1', 'topologyTest', 'F', '1.5', '0', '100')";
		
		String crB2 = "INSERT INTO operators_cr VALUES('2', 'topologyTest', 'B', '0', '0', '3330')";
		String crC2 = "INSERT INTO operators_cr VALUES('2', 'topologyTest', 'C', '0', '0', '2000')";
		String crD2 = "INSERT INTO operators_cr VALUES('2', 'topologyTest', 'D', '0', '0', '1250')";
		String crE2 = "INSERT INTO operators_cr VALUES('2', 'topologyTest', 'E', '0', '0', '500')";
		String crF2 = "INSERT INTO operators_cr VALUES('2', 'topologyTest', 'F', '0', '0', '100')";
		
		String crB3 = "INSERT INTO operators_cr VALUES('3', 'topologyTest', 'B', '1.5', '0', '1100')";
		String crC3 = "INSERT INTO operators_cr VALUES('3', 'topologyTest', 'C', '2', '0', '900')";
		String crD3 = "INSERT INTO operators_cr VALUES('3', 'topologyTest', 'D', '0.8', '0', '1550')";
		String crE3 = "INSERT INTO operators_cr VALUES('3', 'topologyTest', 'E', '1', '0', '300')";
		String crF3 = "INSERT INTO operators_cr VALUES('3', 'topologyTest', 'F', '0.6', '0', '600')";
		
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
		queries.add(crB1);
		queries.add(crC1);
		queries.add(crD1);
		queries.add(crE1);
		queries.add(crF1);
		queries.add(crB2);
		queries.add(crC2);
		queries.add(crD2);
		queries.add(crE2);
		queries.add(crF2);
		queries.add(crB3);
		queries.add(crC3);
		queries.add(crD3);
		queries.add(crE3);
		queries.add(crF3);
		
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
		String dbUrl = "jdbc:mysql://localhost/benchmarks";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		String cleanQuery1 = "DELETE FROM all_time_spouts_stats";
		String cleanQuery2 = "DELETE FROM all_time_bolts_stats";
		String cleanQuery3 = "DELETE FROM operators_cr";
		String cleanQuery4 = "DELETE FROM topologies_status";
		
		ArrayList<String> queries = new ArrayList<>();
		queries.add(cleanQuery1);
		queries.add(cleanQuery2);
		queries.add(cleanQuery3);
		queries.add(cleanQuery4);
		
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
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyInput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetTopologyInput() throws ClassNotFoundException, SQLException{
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 10.0);
		expected.put(2, 10.0);
		expected.put(3, 15.0);
		
		assertEquals(expected, source.getTopologyInput().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyThroughput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyThroughput() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 5.0);
		expected.put(2, 5.0);
		expected.put(3, 2.0);
		
		assertEquals(expected, source.getTopologyThroughput().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyDephase()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyLosses() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 0.0);
		expected.put(2, 5.0);
		expected.put(3, 1.0);
		
		assertEquals(expected, source.getTopologyDephase().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyLatency()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyLatency() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 10.0);
		expected.put(2, 22.0);
		expected.put(3, 8.0);
		
		assertEquals(expected, source.getTopologyLatency().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyNbExecutors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbExecutors() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 6.0);
		expected.put(2, 7.0);
		expected.put(3, 8.0);
		
		assertEquals(expected, source.getTopologyNbExecutors().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyNbSupervisors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbSupervisors() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 3.0);
		expected.put(2, 3.0);
		expected.put(3, 4.0);
		
		assertEquals(expected, source.getTopologyNbSupervisors().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyNbWorkers()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbWorkers() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 5.0);
		expected.put(2, 6.0);
		expected.put(3, 7.0);
		
		assertEquals(expected, source.getTopologyNbWorkers().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyStatus()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyStatus() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 2.0);
		expected.put(2, 0.0);
		expected.put(3, 1.0);
		
		assertEquals(expected, source.getTopologyStatus().get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getTopologyTraffic(visualizer.structure.IStructure)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetTopologyTraffic() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 13.0);
		expected.put(2, 25.0);
		expected.put(3, 27.0);
		
		assertEquals(expected, source.getTopologyTraffic(structure).get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltInput(java.lang.String, visualizer.structure.IStructure)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltInput() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 10.0);
		expectedB.put(2, 10.0);
		expectedB.put(3, 15.0);
		
		HashMap<Integer, Double> expectedC = new HashMap<>();
		expectedC.put(1, 8.0);
		expectedC.put(2, 20.0);
		expectedC.put(3, 4.0);
		
		assertEquals(expectedB, source.getBoltInput("B", structure).get("topologyTest"));
		assertEquals(expectedC, source.getBoltInput("C", structure).get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltExecuted(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltExecuted() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 10.0);
		expectedB.put(2, 35.0);
		expectedB.put(3, 5.0);
		
		assertEquals(expectedB, source.getBoltExecuted("B").get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltOutputs(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltOutputs() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 8.0);
		expectedB.put(2, 20.0);
		expectedB.put(3, 4.0);
		
		assertEquals(expectedB, source.getBoltOutputs("B").get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltLatency(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltLatency() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 3.0);
		expectedB.put(2, 3.5);
		expectedB.put(3, 4.25);
		
		assertEquals(expectedB, source.getBoltLatency("B").get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltProcessingRate(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltProcessingRate() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 3330.0);
		expectedB.put(2, 3330.0);
		expectedB.put(3, 1100.0);
		
		assertEquals(expectedB, source.getBoltProcessingRate("B").get("topologyTest"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcSource#getBoltCR(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltEPR() throws ClassNotFoundException, SQLException {
		JdbcSource source = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 0.8);
		expectedB.put(2, 0.0);
		expectedB.put(3, 1.5);
		
		assertEquals(expectedB, source.getBoltCR("B").get("topologyTest"));
	}

}