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
	private ArrayList<String> topologies;
	private ArrayList<Integer> streamTypes;
	
	/*Database parameters*/
	private String db_host;
	private String db_name;
	private String db_user;
	private String db_pwd;

	/*Structure parameters*/
	private ArrayList<Edge> edges;
	
	/*Chart customization*/
	private String language;
	private boolean draw_shapes;
	private boolean draw_lines;
	private Integer width;
	private Integer height;
	private String font;
	private Integer titleFontSize;
	private Integer axisFontSize;
	private Integer legendFontSize;
	
	public XmlConfigParser(String filename) throws ParserConfigurationException, SAXException, IOException{
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
		this.topologies = new ArrayList<>();
		this.streamTypes = new ArrayList<>();
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
	public ArrayList<String> getTopologies() {
		return topologies;
	}

	/**
	 * @param topology the topology to set
	 */
	public void addTopology(String topology) {
		this.topologies.add(topology);
	}

	/**
	 * @return the streamType
	 */
	public ArrayList<Integer> getStreamTypes() {
		return streamTypes;
	}

	/**
	 * @param streamType the streamType to set
	 */
	public void addStreamType(Integer streamType) {
		this.streamTypes.add(streamType);
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

	/**
	 * @return the language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * @param language the language to set
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * @return the draw_shapes
	 */
	public boolean isDraw_shapes() {
		return draw_shapes;
	}

	/**
	 * @param draw_shapes the draw_shapes to set
	 */
	public void setDraw_shapes(boolean draw_shapes) {
		this.draw_shapes = draw_shapes;
	}

	/**
	 * @return the draw_lines
	 */
	public boolean isDraw_lines() {
		return draw_lines;
	}

	/**
	 * @param draw_lines the draw_lines to set
	 */
	public void setDraw_lines(boolean draw_lines) {
		this.draw_lines = draw_lines;
	}

	/**
	 * @return the width
	 */
	public Integer getWidth() {
		return width;
	}

	/**
	 * @param width the width to set
	 */
	public void setWidth(Integer width) {
		this.width = width;
	}

	/**
	 * @return the height
	 */
	public Integer getHeight() {
		return height;
	}

	/**
	 * @param height the height to set
	 */
	public void setHeight(Integer height) {
		this.height = height;
	}

	/**
	 * @return the font
	 */
	public String getFont() {
		return font;
	}

	/**
	 * @param font the font to set
	 */
	public void setFont(String font) {
		this.font = font;
	}

	/**
	 * @return the titleFontSize
	 */
	public Integer getTitleFontSize() {
		return titleFontSize;
	}

	/**
	 * @param titleFontSize the titleFontSize to set
	 */
	public void setTitleFontSize(Integer titleFontSize) {
		this.titleFontSize = titleFontSize;
	}

	/**
	 * @return the axisFontSize
	 */
	public Integer getAxisFontSize() {
		return axisFontSize;
	}

	/**
	 * @param axisFontSize the axisFontSize to set
	 */
	public void setAxisFontSize(Integer axisFontSize) {
		this.axisFontSize = axisFontSize;
	}

	/**
	 * @return the legendFontSize
	 */
	public Integer getLegendFontSize() {
		return legendFontSize;
	}

	/**
	 * @param legendFontSize the legendFontSize to set
	 */
	public void setLegendFontSize(Integer legendFontSize) {
		this.legendFontSize = legendFontSize;
	}

	public void initParameters() {
		Document doc = this.getDocument();
		final Element parameters = (Element) doc.getElementsByTagName(NodeNames.PARAM.toString()).item(0);
		final NodeList command = parameters.getElementsByTagName(NodeNames.COMMAND.toString());
		this.setCommand(command.item(0).getTextContent());
		final NodeList topologies = parameters.getElementsByTagName(NodeNames.TOPOLOGY.toString());
		int nbTopologies = topologies.getLength();
		for(int i = 0; i < nbTopologies; i++){
			this.addTopology(topologies.item(i).getTextContent());
		}
		final NodeList streamTypes = parameters.getElementsByTagName(NodeNames.STREAMTYPE.toString());
		int nbStreamTypes = streamTypes.getLength();
		for(int j = 0; j < nbStreamTypes; j++){
			this.addStreamType(Integer.parseInt(streamTypes.item(j).getTextContent()));
		}
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
		for(int k = 0; k < nbEdges; k++){
			final Element edge = (Element) edges.item(k);
			String source;
			String destination;
			if(edge.hasAttribute(NodeNames.SOURCE.toString()) && edge.hasAttribute(NodeNames.DEST.toString())){
				source = edge.getAttribute(NodeNames.SOURCE.toString());
				destination = edge.getAttribute(NodeNames.DEST.toString());
				Edge edgeStruct = new Edge(source, destination);
				this.edges.add(edgeStruct);
			}
		}
		final NodeList language = parameters.getElementsByTagName(NodeNames.LANG.toString());
		this.setLanguage(language.item(0).getTextContent());
		final NodeList drwShapes = parameters.getElementsByTagName(NodeNames.DRAWSHP.toString());
		this.setDraw_shapes(Boolean.parseBoolean(drwShapes.item(0).getTextContent()));
		final NodeList drwLines = parameters.getElementsByTagName(NodeNames.DRAWLN.toString());
		this.setDraw_lines(Boolean.parseBoolean(drwLines.item(0).getTextContent()));
		final NodeList width = parameters.getElementsByTagName(NodeNames.WIDTH.toString());
		this.setWidth(Integer.parseInt(width.item(0).getTextContent()));
		final NodeList height = parameters.getElementsByTagName(NodeNames.HEIGHT.toString());
		this.setHeight(Integer.parseInt(height.item(0).getTextContent()));
		final NodeList font = parameters.getElementsByTagName(NodeNames.FONT.toString());
		this.setFont(font.item(0).getTextContent());
		final NodeList titleFontSize = parameters.getElementsByTagName(NodeNames.TTLSIZE.toString());
		this.setTitleFontSize(Integer.parseInt(titleFontSize.item(0).getTextContent()));
		final NodeList axisFontSize = parameters.getElementsByTagName(NodeNames.AXSIZE.toString());
		this.setAxisFontSize(Integer.parseInt(axisFontSize.item(0).getTextContent()));
		final NodeList legendFontSize = parameters.getElementsByTagName(NodeNames.LGDSIZE.toString());
		this.setLegendFontSize(Integer.parseInt(legendFontSize.item(0).getTextContent()));
	}
}