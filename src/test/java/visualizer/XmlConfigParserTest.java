/**
 * 
 */
package visualizer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import visualizer.config.XmlConfigParser;
import visualizer.structure.Edge;

/**
 * @author Roland
 *
 */
public class XmlConfigParserTest {

	
	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getFilename()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetFilename() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("parameters.xml", parser.getFilename());
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getCommand()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetCommand() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("ANALYZE", parser.getCommand());
	}
	
	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getTopologies()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetTopology() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("topologyTest1", parser.getTopologies().get(0));
		assertEquals("topologyTest2", parser.getTopologies().get(1));
		assertEquals("topologyTest3", parser.getTopologies().get(2));
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getStreamTypes()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetStreamType() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals(1, parser.getStreamTypes().get(0), 0);
		assertEquals(1, parser.getStreamTypes().get(0), 0);
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getDb_host()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetDb_host() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("localhost", parser.getDb_host());
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getDb_name()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetDb_name() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("benchmarks", parser.getDb_name());
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getDb_user()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetDb_user() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("root", parser.getDb_user());
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getDb_pwd()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetDb_pwd() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		assertEquals("", parser.getDb_pwd());
	}

	/**
	 * Test method for {@link visualizer.config.XmlConfigParser#getEdges()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetEdges() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		
		ArrayList<Edge> expectedEdges = new ArrayList<>();
		expectedEdges.add(new Edge("A", "B"));
		expectedEdges.add(new Edge("B", "C"));
		expectedEdges.add(new Edge("C", "D"));
		expectedEdges.add(new Edge("C", "E"));
		expectedEdges.add(new Edge("E", "F"));
		
		assertEquals(expectedEdges, parser.getEdges());
	}

}