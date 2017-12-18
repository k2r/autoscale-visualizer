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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import visualizer.config.XmlConfigParser;
import visualizer.source.JdbcMergeSource;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class JdbcMergeSourceTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUpBeforeClass() throws Exception {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/autoscale_test";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		String queryTop1A1 = "INSERT INTO all_time_spouts_stats VALUES('1', 'host1', '10', 'topologyTest1', 'A', '1', '1', '10', '10', '5', '5', '0', '0', '10')";
		String queryTop1B1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host1', '20', 'topologyTest1', 'B', '2', '4', '10', '10', '8', '8', '3', '0.8', '20.0')";
		String queryTop1C1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '10', 'topologyTest1', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryTop1D1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest1', 'D', '8', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryTop1E1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest1', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryTop1F1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host3', '10', 'topologyTest1', 'F', '14', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryTop1A2 = "INSERT INTO all_time_spouts_stats VALUES('2', 'host1', '10', 'topologyTest1', 'A', '1', '1', '20', '10', '10', '5', '5', '5', '22')";
		String queryTop1B21 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host1', '20', 'topologyTest1', 'B', '2', '3', '30', '20', '20', '12', '2.5', '0.8', '20.0')";
		String queryTop1B22 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '30', 'topologyTest1', 'B', '4', '4', '25', '15', '16', '8', '3.5', '0.8', '20.0')";
		String queryTop1C2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '10', 'topologyTest1', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryTop1D2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest1', 'D', '8', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryTop1E2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest1', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryTop1F2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '10', 'topologyTest1', 'F', '14', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryTop1A3 = "INSERT INTO all_time_spouts_stats VALUES('3', 'host1', '10', 'topologyTest1', 'A', '1', '1', '35', '15', '12', '2', '6', '1', '8')";
		String queryTop1B3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest1', 'B', '2', '4', '35', '5', '40', '4', '4.25', '0.8', '20.0')";
		String queryTop1C3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '10', 'topologyTest1', 'C', '5', '7', '8', '8', '6', '6', '5', '1', '25.0')";
		String queryTop1D31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '20', 'topologyTest1', 'D', '8', '9', '6', '6', '5', '5', '8', '0.84', '30.0')";
		String queryTop1D32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest1', 'D', '10', '10', '6', '6', '0', '0', '10', '0', '30.0')";
		String queryTop1E3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '30', 'topologyTest1', 'E', '11', '13', '6', '6', '5', '5', '8', '0.84', '35.0')";
		String queryTop1F31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '10', 'topologyTest1', 'F', '14', '15', '5', '5', '0', '0', '20', '0', '40.0')";
		String queryTop1F32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host4', '10', 'topologyTest1', 'F', '16', '16', '5', '5', '0', '0', '20', '0', '40.0')";
		
		String queryTop2A1 = "INSERT INTO all_time_spouts_stats VALUES('111', 'host1', '10', 'topologyTest2', 'A', '1', '1', '15', '15', '10', '10', '5', '5', '15')";
		String queryTop2B1 = "INSERT INTO all_time_bolts_stats VALUES('111', 'host1', '20', 'topologyTest2', 'B', '2', '4', '15', '15', '13', '13', '8', '1.3', '25.0')";
		String queryTop2C1 = "INSERT INTO all_time_bolts_stats VALUES('111', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D1 = "INSERT INTO all_time_bolts_stats VALUES('111', 'host2', '20', 'topologyTest2', 'D', '8', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E1 = "INSERT INTO all_time_bolts_stats VALUES('111', 'host2', '20', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F1 = "INSERT INTO all_time_bolts_stats VALUES('111', 'host3', '10', 'topologyTest2', 'F', '14', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
		String queryTop2A2 = "INSERT INTO all_time_spouts_stats VALUES('233', 'host1', '10', 'topologyTest2', 'A', '1', '1', '25', '15', '15', '10', '10', '10', '27')";
		String queryTop2B21 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host1', '20', 'topologyTest2', 'B', '2', '3', '35', '25', '25', '17', '7.5', '1.3', '25.0')";
		String queryTop2B22 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host3', '30', 'topologyTest2', 'B', '4', '4', '30', '20', '21', '13', '8.5', '1.3', '25.0')";
		String queryTop2C2 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D2 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host2', '20', 'topologyTest2', 'D', '8', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E2 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host2', '20', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F2 = "INSERT INTO all_time_bolts_stats VALUES('233', 'host3', '10', 'topologyTest2', 'F', '14', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
		String queryTop2A3 = "INSERT INTO all_time_spouts_stats VALUES('344', 'host1', '10', 'topologyTest2', 'A', '1', '1', '40', '20', '17', '7', '11', '6', '13')";
		String queryTop2B3 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host3', '20', 'topologyTest2', 'B', '2', '4', '40', '10', '45', '9', '9.25', '1.3', '25.0')";
		String queryTop2C3 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D31 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host2', '20', 'topologyTest2', 'D', '8', '9', '11', '11', '10', '10', '13', '1.34', '35.0')";
		String queryTop2D32 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host3', '20', 'topologyTest2', 'D', '10', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E3 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host2', '30', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F31 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host3', '10', 'topologyTest2', 'F', '14', '15', '10', '11', '5', '5', '25', '0.5', '45.0')";
		String queryTop2F32 = "INSERT INTO all_time_bolts_stats VALUES('344', 'host4', '10', 'topologyTest2', 'F', '16', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
		String queryTop3A1 = "INSERT INTO all_time_spouts_stats VALUES('1', 'host1', '10', 'topologyTest3', 'A', '1', '1', '20', '20', '15', '15', '10', '10', '20')";
		String queryTop3B1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host1', '20', 'topologyTest3', 'B', '2', '4', '20', '20', '18', '18', '13', '1.8', '30.0')";
		String queryTop3C1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '10', 'topologyTest3', 'C', '5', '7', '18', '18', '16', '16', '15', '2', '35.0')";
		String queryTop3D1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest3', 'D', '8', '10', '16', '16', '10', '10', '20', '1', '40.0')";
		String queryTop3E1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest3', 'E', '11', '13', '16', '16', '15', '15', '18', '1.84', '45.0')";
		String queryTop3F1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host3', '10', 'topologyTest3', 'F', '14', '16', '15', '15', '10', '10', '30', '1', '50.0')";
		
		String queryTop3A2 = "INSERT INTO all_time_spouts_stats VALUES('2', 'host1', '10', 'topologyTest3', 'A', '1', '1', '30', '20', '20', '15', '15', '15', '32')";
		String queryTop3B21 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host1', '20', 'topologyTest3', 'B', '2', '3', '40', '30', '30', '22', '12.5', '1.8', '30.0')";
		String queryTop3B22 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '30', 'topologyTest3', 'B', '4', '4', '35', '25', '26', '18', '13.5', '1.8', '30.0')";
		String queryTop3C2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '10', 'topologyTest3', 'C', '5', '7', '18', '18', '16', '16', '15', '2', '35.0')";
		String queryTop3D2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest3', 'D', '8', '10', '16', '16', '10', '10', '20', '1', '40.0')";
		String queryTop3E2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest3', 'E', '11', '13', '16', '16', '15', '15', '18', '1.84', '45.0')";
		String queryTop3F2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '10', 'topologyTest3', 'F', '14', '16', '15', '15', '10', '10', '30', '1', '50.0')";
	
		String queryTop3A3 = "INSERT INTO all_time_spouts_stats VALUES('3', 'host1', '10', 'topologyTest3', 'A', '1', '1', '45', '25', '22', '12', '16', '11', '18')";
		String queryTop3B3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest3', 'B', '2', '4', '45', '15', '50', '14', '14.25', '1.8', '30.0')";
		String queryTop3C3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '10', 'topologyTest3', 'C', '5', '7', '18', '18', '16', '16', '15', '2', '35.0')";
		String queryTop3D31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '20', 'topologyTest3', 'D', '8', '9', '16', '16', '15', '15', '18', '1.84', '40.0')";
		String queryTop3D32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest3', 'D', '10', '10', '16', '16', '10', '10', '20', '1', '40.0')";
		String queryTop3E3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '30', 'topologyTest3', 'E', '11', '13', '16', '16', '15', '15', '18', '1.84', '45.0')";
		String queryTop3F31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '10', 'topologyTest3', 'F', '14', '15', '15', '15', '10', '10', '30', '1', '50.0')";
		String queryTop3F32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host4', '10', 'topologyTest3', 'F', '16', '16', '15', '15', '10', '10', '30', '1', '50.0')";
		
		List<String> queries  = Arrays.asList(queryTop1A1, queryTop1A2, queryTop1A3, queryTop1B1, queryTop1B21, queryTop1B22, queryTop1B3, queryTop1C1, queryTop1C2,
				queryTop1C3, queryTop1D1, queryTop1D2, queryTop1D31, queryTop1D32, queryTop1E1, queryTop1E2, queryTop1E3, queryTop1F1, queryTop1F2, queryTop1F31, queryTop1F32,
				queryTop2A1, queryTop2A2, queryTop2A3, queryTop2B1, queryTop2B21, queryTop2B22, queryTop2B3, queryTop2C1, queryTop2C2, queryTop2C3, queryTop2D1,
				queryTop2D2, queryTop2D31, queryTop2D32, queryTop2E1, queryTop2E2, queryTop2E3, queryTop2F1, queryTop2F2, queryTop2F31, queryTop2F32, queryTop3A1, queryTop3A2,
				queryTop3A3, queryTop3B1, queryTop3B21, queryTop3B22, queryTop3B3, queryTop3C1, queryTop3C2, queryTop3C3, queryTop3D1, queryTop3D2, queryTop3D31, 
				queryTop3D32, queryTop3E1, queryTop3E2, queryTop3E3, queryTop3F1, queryTop3F2, queryTop3F31, queryTop3F32);
		
		
		
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
	@After
	public void tearDownAfterClass() throws Exception {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/autoscale_test";
		Class.forName(jdbcDriver);
		Connection connection = DriverManager.getConnection(dbUrl, "root", null);
		String cleanQuery1 = "DELETE FROM all_time_spouts_stats";
		String cleanQuery2 = "DELETE FROM all_time_bolts_stats";
		ArrayList<String> queries = new ArrayList<>();
		queries.add(cleanQuery1);
		queries.add(cleanQuery2);
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
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyInput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyInput() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 25.0);
		expectedMax.put(110, 15.0);
		expectedMax.put(230, 15.0);
		expectedMax.put(340, 20.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 20.0);
		expectedAvg.put(110, 15.0);
		expectedAvg.put(230, 15.0);
		expectedAvg.put(340, 20.0);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 15.0);
		expectedMin.put(110, 15.0);
		expectedMin.put(230, 15.0);
		expectedMin.put(340, 20.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MAX", expectedMax);
		expected.put("topologyTest_MIN", expectedMin);
		
		assertEquals(expected, source.getTopologyInput());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyThroughput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyThroughput() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 12.0);
		expectedMax.put(110, 10.0);
		expectedMax.put(230, 10.0);
		expectedMax.put(340, 7.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 7.0);
		expectedAvg.put(110, 10.0);
		expectedAvg.put(230, 10.0);
		expectedAvg.put(340, 7.0);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 2.0);
		expectedMin.put(110, 10.0);
		expectedMin.put(230, 10.0);
		expectedMin.put(340, 7.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MAX", expectedMax);
		expected.put("topologyTest_MIN", expectedMin);
		
		assertEquals(expected, source.getTopologyThroughput());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyDephase()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyDephase() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 11.0);
		expectedMax.put(110, 5.0);
		expectedMax.put(230, 10.0);
		expectedMax.put(340, 6.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 6.0);
		expectedAvg.put(110, 5.0);
		expectedAvg.put(230, 10.0);
		expectedAvg.put(340, 6.0);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 1.0);
		expectedMin.put(110, 5.0);
		expectedMin.put(230, 10.0);
		expectedMin.put(340, 6.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MAX", expectedMax);
		expected.put("topologyTest_MIN", expectedMin);
		
		assertEquals(expected, source.getTopologyDephase());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyLatency()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyLatency() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 18.0);
		expectedMax.put(110, 15.0);
		expectedMax.put(230, 27.0);
		expectedMax.put(340, 13.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 13.0);
		expectedAvg.put(110, 15.0);
		expectedAvg.put(230, 27.0);
		expectedAvg.put(340, 13.0);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 8.0);
		expectedMin.put(110, 15.0);
		expectedMin.put(230, 27.0);
		expectedMin.put(340, 13.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MAX", expectedMax);
		expected.put("topologyTest_MIN", expectedMin);
		
		assertEquals(expected, source.getTopologyLatency());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyNbExecutors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbExecutors() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 6.0);
		expectedMin.put(10, 7.0);
		expectedMin.put(20, 8.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 6.0);
		expectedAvg.put(10, 7.0);
		expectedAvg.put(20, 8.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 6.0);
		expectedMax.put(10, 7.0);
		expectedMax.put(20, 8.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getTopologyNbExecutors());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyNbSupervisors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbSupervisors() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 3.0);
		expectedMin.put(10, 3.0);
		expectedMin.put(20, 4.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 3.0);
		expectedAvg.put(10, 3.0);
		expectedAvg.put(20, 4.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 3.0);
		expectedMax.put(10, 3.0);
		expectedMax.put(20, 4.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getTopologyNbSupervisors());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyNbWorkers()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyNbWorkers() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 5.0);
		expectedMin.put(10, 6.0);
		expectedMin.put(20, 7.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 5.0);
		expectedAvg.put(10, 6.0);
		expectedAvg.put(20, 7.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 5.0);
		expectedMax.put(10, 6.0);
		expectedMax.put(20, 7.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getTopologyNbWorkers());
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getTopologyTraffic(visualizer.structure.IStructure)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetTopologyTraffic() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges());
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 13.0);
		expectedMin.put(10, 25.0);
		expectedMin.put(20, 27.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 23.0);
		expectedAvg.put(10, 40.0);
		expectedAvg.put(20, 44.5);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 33.0);
		expectedMax.put(10, 55.0);
		expectedMax.put(20, 62.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getTopologyTraffic(structure));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltInput(java.lang.String, visualizer.structure.IStructure)}.
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
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 10.0);
		expectedMin.put(10, 10.0);
		expectedMin.put(20, 15.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 15.0);
		expectedAvg.put(10, 15.0);
		expectedAvg.put(20, 20.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 20.0);
		expectedMax.put(10, 20.0);
		expectedMax.put(20, 25.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltInput("B", structure));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltExecuted(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltExecuted() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 5.0);
		expectedMin.put(110, 15.0);
		expectedMin.put(230, 45.0);
		expectedMin.put(340, 10.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 10.0);
		expectedAvg.put(110, 15.0);
		expectedAvg.put(230, 45.0);
		expectedAvg.put(340, 10.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 15.0);
		expectedMax.put(110, 15.0);
		expectedMax.put(230, 45.0);
		expectedMax.put(340, 10.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltExecuted("B"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltOutputs(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltOutputs() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 4.0);
		expectedMin.put(110, 13.0);
		expectedMin.put(230, 30.0);
		expectedMin.put(340, 9.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 9.0);
		expectedAvg.put(110, 13.0);
		expectedAvg.put(230, 30.0);
		expectedAvg.put(340, 9.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 14.0);
		expectedMax.put(110, 13.0);
		expectedMax.put(230, 30.0);
		expectedMax.put(340, 9.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltOutputs("B"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltLatency(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltLatency() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 4.25);
		expectedMin.put(110, 8.0);
		expectedMin.put(230, 8.5);
		expectedMin.put(340, 9.25);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 9.25);
		expectedAvg.put(110, 8.0);
		expectedAvg.put(230, 8.5);
		expectedAvg.put(340, 9.25);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 14.25);
		expectedMax.put(110, 8.0);
		expectedMax.put(230, 8.5);
		expectedMax.put(340, 9.25);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltLatency("B"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltCpuUsage(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltCpuUsage() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 20.0);
		expectedMin.put(110, 25.0);
		expectedMin.put(230, 50.0);
		expectedMin.put(340, 25.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 25.0);
		expectedAvg.put(110, 25.0);
		expectedAvg.put(230, 50.0);
		expectedAvg.put(340, 25.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 30.0);
		expectedMax.put(110, 25.0);
		expectedMax.put(230, 50.0);
		expectedMax.put(340, 25.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltCpuUsage("B"));
	}

	/**
	 * Test method for {@link visualizer.source.JdbcMergeSource#getBoltRebalancing(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetBoltRebalancing() throws ClassNotFoundException, SQLException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest", 0, Integer.MAX_VALUE);
		HashMap<Integer, Double> expectedMin = new HashMap<>();
		expectedMin.put(0, 1.0);
		expectedMin.put(110, 1.0);
		expectedMin.put(230, 2.0);
		expectedMin.put(340, 1.0);
		HashMap<Integer, Double> expectedAvg = new HashMap<>();
		expectedAvg.put(0, 1.0);
		expectedAvg.put(110, 1.0);
		expectedAvg.put(230, 2.0);
		expectedAvg.put(340, 1.0);
		HashMap<Integer, Double> expectedMax = new HashMap<>();
		expectedMax.put(0, 1.0);
		expectedMax.put(110, 1.0);
		expectedMax.put(230, 2.0);
		expectedMax.put(340, 1.0);
		HashMap<String, HashMap<Integer, Double>> expected = new HashMap<>();
		expected.put("topologyTest", expectedAvg);
		expected.put("topologyTest_MIN", expectedMin);
		expected.put("topologyTest_MAX", expectedMax);
		
		assertEquals(expected, source.getBoltRebalancing("B"));
	}

}
