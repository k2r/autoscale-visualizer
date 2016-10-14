/**
 * 
 */
package visualizer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
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
import visualizer.draw.JFreePainter;
import visualizer.source.FileSource;
import visualizer.source.JdbcSource;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class FileSourceTest {

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
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		
		
		JdbcSource jdbcSource1 = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		JFreePainter painter1 = new JFreePainter("topologyTest1", 1, jdbcSource1);
		painter1.drawTopologyInput();
		painter1.drawTopologyThroughput();
		painter1.drawTopologyLosses();
		painter1.drawTopologyLatency();
		painter1.drawTopologyNbExecutors();
		painter1.drawTopologyNbSupervisors();
		painter1.drawTopologyNbWorkers();
		painter1.drawTopologyStatus();
		painter1.drawTopologyTraffic(structure);
		ArrayList<String> bolts1 = structure.getBolts();
		for(String bolt : bolts1){
			painter1.drawBoltInput(bolt, structure);
			painter1.drawBoltExecuted(bolt);
			painter1.drawBoltOutputs(bolt);
			painter1.drawBoltLatency(bolt);
			painter1.drawBoltCapacity(bolt);
			painter1.drawBoltActivity(bolt);
		}
		
		JdbcSource jdbcSource2 = new JdbcSource("localhost", "benchmarks", "root", null, "topologyTest");
		
		JFreePainter painter2 = new JFreePainter("topologyTest2", 1, jdbcSource2);
		painter2.drawTopologyInput();
		painter2.drawTopologyThroughput();
		painter2.drawTopologyLosses();
		painter2.drawTopologyLatency();
		painter2.drawTopologyNbExecutors();
		painter2.drawTopologyNbSupervisors();
		painter2.drawTopologyNbWorkers();
		painter2.drawTopologyStatus();
		painter2.drawTopologyTraffic(structure);
		ArrayList<String> bolts2 = structure.getBolts();
		for(String bolt : bolts2){
			painter2.drawBoltInput(bolt, structure);
			painter2.drawBoltExecuted(bolt);
			painter2.drawBoltOutputs(bolt);
			painter2.drawBoltLatency(bolt);
			painter2.drawBoltCapacity(bolt);
			painter2.drawBoltActivity(bolt);
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
		
		File file1 = new File("topologyTest1_linear_increase");
		File file2 = new File("topologyTest2_linear_increase");
		FileSourceTest.delete(file1);
		FileSourceTest.delete(file2);
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyInput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyInput() throws ClassNotFoundException, SQLException {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 10.0);
		expected.put(2, 10.0);
		expected.put(3, 15.0);
		
		assertEquals(expected, source.getTopologyInput().get("topologyTest1"));
		assertEquals(expected, source.getTopologyInput().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyThroughput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyThroughput() throws ClassNotFoundException, SQLException {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 5.0);
		expected.put(2, 5.0);
		expected.put(3, 2.0);
		
		assertEquals(expected, source.getTopologyThroughput().get("topologyTest1"));
		assertEquals(expected, source.getTopologyThroughput().get("topologyTest2"));
		
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyLosses()}.
	 */
	@Test
	public void testGetTopologyLosses() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 0.0);
		expected.put(2, 5.0);
		expected.put(3, 1.0);
		
		assertEquals(expected, source.getTopologyLosses().get("topologyTest1"));
		assertEquals(expected, source.getTopologyLosses().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyLatency()}.
	 */
	@Test
	public void testGetTopologyLatency() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 10.0);
		expected.put(2, 22.0);
		expected.put(3, 8.0);
		
		assertEquals(expected, source.getTopologyLatency().get("topologyTest1"));
		assertEquals(expected, source.getTopologyLatency().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyNbExecutors()}.
	 */
	@Test
	public void testGetTopologyNbExecutors() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 6.0);
		expected.put(2, 7.0);
		expected.put(3, 8.0);
		
		assertEquals(expected, source.getTopologyNbExecutors().get("topologyTest1"));
		assertEquals(expected, source.getTopologyNbExecutors().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyNbSupervisors()}.
	 */
	@Test
	public void testGetTopologyNbSupervisors() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 3.0);
		expected.put(2, 3.0);
		expected.put(3, 4.0);
		
		assertEquals(expected, source.getTopologyNbSupervisors().get("topologyTest1"));
		assertEquals(expected, source.getTopologyNbSupervisors().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyNbWorkers()}.
	 */
	@Test
	public void testGetTopologyNbWorkers() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 5.0);
		expected.put(2, 6.0);
		expected.put(3, 7.0);
		
		assertEquals(expected, source.getTopologyNbWorkers().get("topologyTest1"));
		assertEquals(expected, source.getTopologyNbWorkers().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyStatus()}.
	 */
	@Test
	public void testGetTopologyStatus() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 2.0);
		expected.put(2, 0.0);
		expected.put(3, 1.0);
		
		assertEquals(expected, source.getTopologyStatus().get("topologyTest1"));
		assertEquals(expected, source.getTopologyStatus().get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getTopologyTraffic(visualizer.structure.IStructure)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetTopologyTraffic() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expected = new HashMap<>();
		expected.put(1, 13.0);
		expected.put(2, 25.0);
		expected.put(3, 27.0);
		
		assertEquals(expected, source.getTopologyTraffic(structure).get("topologyTest1"));
		assertEquals(expected, source.getTopologyTraffic(structure).get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltInput(java.lang.String, visualizer.structure.IStructure)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetBoltInput() throws ParserConfigurationException, SAXException, IOException {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 10.0);
		expectedB.put(2, 10.0);
		expectedB.put(3, 15.0);
		
		HashMap<Integer, Double> expectedC = new HashMap<>();
		expectedC.put(1, 8.0);
		expectedC.put(2, 20.0);
		expectedC.put(3, 4.0);
		
		assertEquals(expectedB, source.getBoltInput("B", structure).get("topologyTest1"));
		assertEquals(expectedC, source.getBoltInput("C", structure).get("topologyTest1"));
		assertEquals(expectedB, source.getBoltInput("B", structure).get("topologyTest2"));
		assertEquals(expectedC, source.getBoltInput("C", structure).get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltExecuted(java.lang.String)}.
	 */
	@Test
	public void testGetBoltExecuted() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 10.0);
		expectedB.put(2, 35.0);
		expectedB.put(3, 5.0);
		
		assertEquals(expectedB, source.getBoltExecuted("B").get("topologyTest1"));
		assertEquals(expectedB, source.getBoltExecuted("B").get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltOutputs(java.lang.String)}.
	 */
	@Test
	public void testGetBoltOutputs() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 8.0);
		expectedB.put(2, 20.0);
		expectedB.put(3, 4.0);
		
		assertEquals(expectedB, source.getBoltOutputs("B").get("topologyTest1"));
		assertEquals(expectedB, source.getBoltOutputs("B").get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltLatency(java.lang.String)}.
	 */
	@Test
	public void testGetBoltLatency() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 3.0);
		expectedB.put(2, 3.5);
		expectedB.put(3, 4.25);
		
		assertEquals(expectedB, source.getBoltLatency("B").get("topologyTest1"));
		assertEquals(expectedB, source.getBoltLatency("B").get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltProcessingRate(java.lang.String)}.
	 */
	@Test
	public void testGetBoltProcessingRate() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 3330.0);
		expectedB.put(2, 3330.0);
		expectedB.put(3, 1100.0);
		
		assertEquals(expectedB, source.getBoltProcessingRate("B").get("topologyTest1"));
		assertEquals(expectedB, source.getBoltProcessingRate("B").get("topologyTest2"));
	}

	/**
	 * Test method for {@link visualizer.source.FileSource#getBoltCR(java.lang.String)}.
	 */
	@Test
	public void testGetBoltEPR() {
		ArrayList<String> topologies = new ArrayList<>();
		topologies.add("topologyTest1");
		topologies.add("topologyTest2");
		ArrayList<Integer> varCodes = new ArrayList<>();
		varCodes.add(1);
		varCodes.add(1);
		
		FileSource source = new FileSource(topologies, varCodes);
		HashMap<Integer, Double> expectedB = new HashMap<>();
		expectedB.put(1, 0.8);
		expectedB.put(2, 0.0);
		expectedB.put(3, 1.5);
		
		assertEquals(expectedB, source.getBoltCR("B").get("topologyTest1"));
		assertEquals(expectedB, source.getBoltCR("B").get("topologyTest2"));
	}
}