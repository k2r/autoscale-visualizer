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
	
	/*Visualizer global parameters*/
	private String topology;
	private Integer streamType;
	
	/*Database parameters*/
	private String db_host;
	private String db_name;
	private String db_user;
	private String db_pwd;
	
	/*Structure parameters*/
	private ArrayList<Edge> edges;
	
	public XmlConfigParser(String filename) throws ParserConfigurationException, SAXException, IOException{
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
		this.edges = new ArrayList<>();
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
	 * @return the topology
	 */
	public String getTopology() {
		return topology;
	}

	/**
	 * @param topology the topology to set
	 */
	public void setTopology(String topology) {
		this.topology = topology;
	}

	/**
	 * @return the streamType
	 */
	public Integer getStreamType() {
		return streamType;
	}

	/**
	 * @param streamType the streamType to set
	 */
	public void setStreamType(Integer streamType) {
		this.streamType = streamType;
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
	public ArrayList<Edge> getEdges() {
		return edges;
	}

	/**
	 * @param edges the edges to set
	 */
	public void setEdges(ArrayList<Edge> edges) {
		this.edges = edges;
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
		final NodeList topology = parameters.getElementsByTagName(NodeNames.TOPOLOGY.toString());
		this.setTopology(topology.item(0).getTextContent());
		final NodeList streamType = parameters.getElementsByTagName(NodeNames.STREAMTYPE.toString());
		this.setStreamType(Integer.parseInt(streamType.item(0).getTextContent()));
		final NodeList dbHost = parameters.getElementsByTagName(NodeNames.DBHOST.toString());
		this.setDb_host(dbHost.item(0).getTextContent());
		final NodeList dbName = parameters.getElementsByTagName(NodeNames.DBNAME.toString());
		this.setDb_name(dbName.item(0).getTextContent());
		final NodeList dbUser = parameters.getElementsByTagName(NodeNames.DBUSER.toString());
		this.setDb_user(dbUser.item(0).getTextContent());
		final NodeList dbPwd = parameters.getElementsByTagName(NodeNames.DBPWD.toString());
		this.setDb_pwd(dbPwd.item(0).getTextContent());
		final NodeList edges = parameters.getElementsByTagName(NodeNames.EDGE.toString());
		int nbEdges = edges.getLength();
		for(int i = 0; i < nbEdges; i++){
			final Element edge = (Element) edges.item(i);
			String source;
			String destination;
			if(edge.hasAttribute(NodeNames.SOURCE.toString()) && edge.hasAttribute(NodeNames.DEST.toString())){
				source = edge.getAttribute(NodeNames.SOURCE.toString());
				destination = edge.getAttribute(NodeNames.DEST.toString());
				Edge edgeStruct = new Edge(source, destination);
				this.edges.add(edgeStruct);
			}
		}
	}
}
