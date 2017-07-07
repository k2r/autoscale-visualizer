/**
 * 
 */
package visualizer;

//import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import visualizer.config.LabelParser;
import visualizer.config.XmlConfigParser;
import visualizer.draw.JFreeMergePainter;
import visualizer.source.JdbcMergeSource;
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class JFreeMergePainterTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
		
		String queryTop2A1 = "INSERT INTO all_time_spouts_stats VALUES('1', 'host1', '10', 'topologyTest2', 'A', '1', '1', '15', '15', '10', '10', '5', '5', '15')";
		String queryTop2B1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host1', '20', 'topologyTest2', 'B', '2', '4', '15', '15', '13', '13', '8', '1.3', '25.0')";
		String queryTop2C1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest2', 'D', '8', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host2', '20', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F1 = "INSERT INTO all_time_bolts_stats VALUES('1', 'host3', '10', 'topologyTest2', 'F', '14', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
		String queryTop2A2 = "INSERT INTO all_time_spouts_stats VALUES('2', 'host1', '10', 'topologyTest2', 'A', '1', '1', '25', '15', '15', '10', '10', '10', '27')";
		String queryTop2B21 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host1', '20', 'topologyTest2', 'B', '2', '3', '35', '25', '25', '17', '7.5', '1.3', '25.0')";
		String queryTop2B22 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '30', 'topologyTest2', 'B', '4', '4', '30', '20', '21', '13', '8.5', '1.3', '25.0')";
		String queryTop2C2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest2', 'D', '8', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host2', '20', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F2 = "INSERT INTO all_time_bolts_stats VALUES('2', 'host3', '10', 'topologyTest2', 'F', '14', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
		String queryTop2A3 = "INSERT INTO all_time_spouts_stats VALUES('3', 'host1', '10', 'topologyTest2', 'A', '1', '1', '40', '20', '17', '7', '11', '6', '13')";
		String queryTop2B3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest2', 'B', '2', '4', '40', '10', '45', '9', '9.25', '1.3', '25.0')";
		String queryTop2C3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '10', 'topologyTest2', 'C', '5', '7', '13', '13', '11', '11', '10', '1.5', '30.0')";
		String queryTop2D31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '20', 'topologyTest2', 'D', '8', '9', '11', '11', '10', '10', '13', '1.34', '35.0')";
		String queryTop2D32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '20', 'topologyTest2', 'D', '10', '10', '11', '11', '5', '5', '15', '0.5', '35.0')";
		String queryTop2E3 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host2', '30', 'topologyTest2', 'E', '11', '13', '11', '11', '10', '10', '13', '1.34', '40.0')";
		String queryTop2F31 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host3', '10', 'topologyTest2', 'F', '14', '15', '10', '11', '5', '5', '25', '0.5', '45.0')";
		String queryTop2F32 = "INSERT INTO all_time_bolts_stats VALUES('3', 'host4', '10', 'topologyTest2', 'F', '16', '16', '10', '10', '5', '5', '25', '0.5', '45.0')";
		
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
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
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
		
		File file1 = new File("topologyTest_linear_increase");
		FileSourceTest.delete(file1);
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyInput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyInput() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyInput();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyThroughput()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyThroughput() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyThroughput();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyLosses()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyLosses() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyLosses();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyLatency()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyLatency() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyLatency();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyNbExecutors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbExecutors() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbExecutors();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyNbSupervisors()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbSupervisors() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbSupervisors();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyNbWorkers()}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyNbWorkers() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyNbWorkers();
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawTopologyTraffic(visualizer.structure.IStructure)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawTopologyTraffic() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		painter.drawTopologyTraffic(structure);
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltInput(java.lang.String, visualizer.structure.IStructure)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltInput() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltInput(bolt, structure);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltExecuted(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltExecuted() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltExecuted(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltOutputs(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltOutputs() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltOutputs(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltLatency(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltLatency() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltLatency(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltCpuUsage(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltCpuUsage() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltCpuUsage(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltRebalancing(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltRebalancing() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltRebalancing(bolt);
		}
	}

	/**
	 * Test method for {@link visualizer.draw.JFreeMergePainter#drawBoltPending(java.lang.String)}.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testDrawBoltPending() throws ClassNotFoundException, SQLException, ParserConfigurationException, SAXException, IOException {
		JdbcMergeSource source = new JdbcMergeSource("localhost", "autoscale_test", "root", null, "topologyTest");
		XmlConfigParser cp = new XmlConfigParser("parameters.xml");
		cp.initParameters();
		LabelParser lp = new LabelParser(cp.getLanguage());
		lp.initParameters();
		TopologyStructure structure = new TopologyStructure(cp.getEdges());
		JFreeMergePainter painter = new JFreeMergePainter("topologyTest", 1, source, cp, lp);
		for(String bolt : structure.getBolts()){
			painter.drawBoltPending(bolt);
		}
	}
}