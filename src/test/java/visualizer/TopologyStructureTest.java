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
import visualizer.structure.TopologyStructure;

/**
 * @author Roland
 *
 */
public class TopologyStructureTest {

	/**
	 * Test method for {@link visualizer.structure.TopologyStructure#getComponents()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetComponents() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges1());
		ArrayList<String> expectedComponents = new ArrayList<>();
		expectedComponents.add("A");
		expectedComponents.add("B");
		expectedComponents.add("C");
		expectedComponents.add("D");
		expectedComponents.add("E");
		expectedComponents.add("F");
		
		assertEquals(expectedComponents, structure.getComponents());
	}

	/**
	 * Test method for {@link visualizer.structure.TopologyStructure#getSpouts()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetSpouts() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges1());
		ArrayList<String> expectedSpouts = new ArrayList<>();
		expectedSpouts.add("A");
		
		assertEquals(expectedSpouts, structure.getSpouts());
	}

	/**
	 * Test method for {@link visualizer.structure.TopologyStructure#getBolts()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetBolts() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges1());
		ArrayList<String> expectedBolts = new ArrayList<>();
		expectedBolts.add("B");
		expectedBolts.add("C");
		expectedBolts.add("D");
		expectedBolts.add("E");
		expectedBolts.add("F");
		
		assertEquals(expectedBolts, structure.getBolts());
	}

	/**
	 * Test method for {@link visualizer.structure.TopologyStructure#getParents(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetParents() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges1());
		ArrayList<String> expectedParentsA = new ArrayList<>();
		ArrayList<String> expectedParentsB = new ArrayList<>();
		expectedParentsB.add("A");
		ArrayList<String> expectedParentsC = new ArrayList<>();
		expectedParentsC.add("B");
		ArrayList<String> expectedParentsD = new ArrayList<>();
		expectedParentsD.add("C");
		ArrayList<String> expectedParentsE = new ArrayList<>();
		expectedParentsE.add("C");
		ArrayList<String> expectedParentsF = new ArrayList<>();
		expectedParentsF.add("E");
		
		assertEquals(expectedParentsA, structure.getParents("A"));
		assertEquals(expectedParentsB, structure.getParents("B"));
		assertEquals(expectedParentsC, structure.getParents("C"));
		assertEquals(expectedParentsD, structure.getParents("D"));
		assertEquals(expectedParentsE, structure.getParents("E"));
		assertEquals(expectedParentsF, structure.getParents("F"));
	}
	
	/**
	 * Test method for {@link visualizer.structure.TopologyStructure#getParents(java.lang.String)}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	@Test
	public void testGetChildren() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("parameters.xml");
		parser.initParameters();
		TopologyStructure structure = new TopologyStructure(parser.getEdges1());
		ArrayList<String> expectedChildrenA = new ArrayList<>();
		expectedChildrenA.add("B");
		ArrayList<String> expectedChildrenB = new ArrayList<>();
		expectedChildrenB.add("C");
		ArrayList<String> expectedChildrenC = new ArrayList<>();
		expectedChildrenC.add("D");
		expectedChildrenC.add("E");
		ArrayList<String> expectedChildrenD = new ArrayList<>();
		ArrayList<String> expectedChildrenE = new ArrayList<>();
		expectedChildrenE.add("F");
		ArrayList<String> expectedChildrenF = new ArrayList<>();
		
		assertEquals(expectedChildrenA, structure.getChildren("A"));
		assertEquals(expectedChildrenB, structure.getChildren("B"));
		assertEquals(expectedChildrenC, structure.getChildren("C"));
		assertEquals(expectedChildrenD, structure.getChildren("D"));
		assertEquals(expectedChildrenE, structure.getChildren("E"));
		assertEquals(expectedChildrenF, structure.getChildren("F"));
	}
}