/**
 * 
 */
package visualizer.config;

import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import visualizer.structure.Edge;

/**
 * @author Roland
 *
 */
public class XmlConfigParser {

	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	/*Visualizer command*/
	private String command;
	
	/*Visualizer global parameters*/
	private String topology1;
	private Integer streamType1;
	private String topology2;
	private Integer streamType2;
	
	/*Database parameters*/
	private String db_host;
	private String db_name;
	private String db_user;
	private String db_pwd;
	
	/*Structure parameters*/
	private ArrayList<Edge> edges1;
	private ArrayList<Edge> edges2;
	
	public XmlConfigParser(String filename) throws ParserConfigurationException, SAXException, IOException{
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
		this.edges1 = new ArrayList<>();
		this.edges2 = new ArrayList<>();
	}
	
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the command
	 */
	public String getCommand() {
		return command;
	}

	/**
	 * @param command the command to set
	 */
	public void setCommand(String command) {
		this.command = command;
	}

	/**
	 * @return the topology
	 */
	public String getTopology1() {
		return topology1;
	}

	/**
	 * @param topology the topology to set
	 */
	public void setTopology1(String topology) {
		this.topology1 = topology;
	}

	/**
	 * @return the topology2
	 */
	public String getTopology2() {
		return topology2;
	}

	/**
	 * @param topology2 the topology2 to set
	 */
	public void setTopology2(String topology2) {
		this.topology2 = topology2;
	}

	/**
	 * @return the streamType
	 */
	public Integer getStreamType1() {
		return streamType1;
	}

	/**
	 * @param streamType the streamType to set
	 */
	public void setStreamType1(Integer streamType) {
		this.streamType1 = streamType;
	}

	/**
	 * @return the streamType2
	 */
	public Integer getStreamType2() {
		return streamType2;
	}

	/**
	 * @param streamType2 the streamType2 to set
	 */
	public void setStreamType2(Integer streamType2) {
		this.streamType2 = streamType2;
	}

	/**
	 * @return the db_host
	 */
	public String getDb_host() {
		return db_host;
	}

	/**
	 * @param db_host the db_host to set
	 */
	public void setDb_host(String db_host) {
		this.db_host = db_host;
	}

	/**
	 * @return the db_name
	 */
	public String getDb_name() {
		return db_name;
	}

	/**
	 * @param db_name the db_name to set
	 */
	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}

	/**
	 * @return the db_user
	 */
	public String getDb_user() {
		return db_user;
	}

	/**
	 * @param db_user the db_user to set
	 */
	public void setDb_user(String db_user) {
		this.db_user = db_user;
	}

	/**
	 * @return the db_pwd
	 */
	public String getDb_pwd() {
		return db_pwd;
	}

	/**
	 * @param db_pwd the db_pwd to set
	 */
	public void setDb_pwd(String db_pwd) {
		this.db_pwd = db_pwd;
	}

	/**
	 * @return the edges
	 */
	public ArrayList<Edge> getEdges1() {
		return edges1;
	}

	/**
	 * @param edges the edges to set
	 */
	public void setEdges1(ArrayList<Edge> edges) {
		this.edges1 = edges;
	}

	/**
	 * @return the edges2
	 */
	public ArrayList<Edge> getEdges2() {
		return edges2;
	}

	/**
	 * @param edges2 the edges2 to set
	 */
	public void setEdges2(ArrayList<Edge> edges2) {
		this.edges2 = edges2;
	}

	/**
	 * @return the document
	 */
	public Document getDocument() {
		return document;
	}

	public void initParameters() {
		Document doc = this.getDocument();
		final Element parameters = (Element) doc.getElementsByTagName(NodeNames.PARAM.toString()).item(0);
		final NodeList command = parameters.getElementsByTagName(NodeNames.COMMAND.toString());
		this.setCommand(command.item(0).getTextContent());
		final NodeList topology1 = parameters.getElementsByTagName(NodeNames.TOPOLOGY1.toString());
		this.setTopology1(topology1.item(0).getTextContent());
		final NodeList topology2 = parameters.getElementsByTagName(NodeNames.TOPOLOGY2.toString());
		this.setTopology2(topology2.item(0).getTextContent());
		final NodeList streamType1 = parameters.getElementsByTagName(NodeNames.STREAMTYPE1.toString());
		this.setStreamType1(Integer.parseInt(streamType1.item(0).getTextContent()));
		final NodeList streamType2 = parameters.getElementsByTagName(NodeNames.STREAMTYPE2.toString());
		this.setStreamType2(Integer.parseInt(streamType2.item(0).getTextContent()));
		final NodeList dbHost = parameters.getElementsByTagName(NodeNames.DBHOST.toString());
		this.setDb_host(dbHost.item(0).getTextContent());
		final NodeList dbName = parameters.getElementsByTagName(NodeNames.DBNAME.toString());
		this.setDb_name(dbName.item(0).getTextContent());
		final NodeList dbUser = parameters.getElementsByTagName(NodeNames.DBUSER.toString());
		this.setDb_user(dbUser.item(0).getTextContent());
		final NodeList dbPwd = parameters.getElementsByTagName(NodeNames.DBPWD.toString());
		this.setDb_pwd(dbPwd.item(0).getTextContent());
		final NodeList edges1 = parameters.getElementsByTagName(NodeNames.EDGE1.toString());
		int nbEdges1 = edges1.getLength();
		for(int i = 0; i < nbEdges1; i++){
			final Element edge = (Element) edges1.item(i);
			String source;
			String destination;
			if(edge.hasAttribute(NodeNames.SOURCE.toString()) && edge.hasAttribute(NodeNames.DEST.toString())){
				source = edge.getAttribute(NodeNames.SOURCE.toString());
				destination = edge.getAttribute(NodeNames.DEST.toString());
				Edge edgeStruct = new Edge(source, destination);
				this.edges1.add(edgeStruct);
			}
		}
		final NodeList edges2 = parameters.getElementsByTagName(NodeNames.EDGE2.toString());
		int nbEdges2 = edges2.getLength();
		for(int i = 0; i < nbEdges2; i++){
			final Element edge = (Element) edges2.item(i);
			String source;
			String destination;
			if(edge.hasAttribute(NodeNames.SOURCE.toString()) && edge.hasAttribute(NodeNames.DEST.toString())){
				source = edge.getAttribute(NodeNames.SOURCE.toString());
				destination = edge.getAttribute(NodeNames.DEST.toString());
				Edge edgeStruct = new Edge(source, destination);
				this.edges2.add(edgeStruct);
			}
		}
	}
}