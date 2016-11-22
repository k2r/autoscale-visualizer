/**
 * 
 */
package visualizer.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Roland
 *
 */
public class LabelParser {
	
	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	private HashMap<String, String> titles;
	private HashMap<String, String> xaxisLabels;
	private HashMap<String, String> yaxisLabels;
	
	public LabelParser(String lang) throws ParserConfigurationException, SAXException, IOException{
		if(lang.equalsIgnoreCase("FR")){
			this.filename = "./visualizer_lang/labels_FR.xml";
		}else if(lang.equalsIgnoreCase("ENG")){
				this.filename = "./visualizer_lang/labels_ENG.xml";
		}else{
			System.out.println("The language " + lang + " is not supported by the benchmark visualizer, please select a supported one (FR/ENG).");
		}
		
		this.titles = new HashMap<>();
		this.xaxisLabels = new HashMap<>();
		this.yaxisLabels = new HashMap<>();
		
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());	
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
	 * @return the document
	 */
	public Document getDocument() {
		return document;
	}

	/**
	 * @return the titles
	 */
	public HashMap<String, String> getTitles() {
		return titles;
	}

	/**
	 * @param titles the titles to set
	 */
	public void setTitles(HashMap<String, String> titles) {
		this.titles = titles;
	}

	/**
	 * @return the xaxisLabels
	 */
	public HashMap<String, String> getXaxisLabels() {
		return xaxisLabels;
	}

	/**
	 * @param xaxisLabels the xaxisLabels to set
	 */
	public void setXaxisLabels(HashMap<String, String> xaxisLabels) {
		this.xaxisLabels = xaxisLabels;
	}

	/**
	 * @return the yaxisLabels
	 */
	public HashMap<String, String> getYaxisLabels() {
		return yaxisLabels;
	}

	/**
	 * @param yaxisLabels the yaxisLabels to set
	 */
	public void setYaxisLabels(HashMap<String, String> yaxisLabels) {
		this.yaxisLabels = yaxisLabels;
	}
	
	public String getTitle(String topic){
		return this.titles.get(topic);
	}

	public String getXAxisLabel(String topic){
		return this.xaxisLabels.get(topic);
	}
	
	public String getYAxisLabel(String topic){
		return this.yaxisLabels.get(topic);
	}
	
	public void setTitle(String topic, String value){
		this.titles.put(topic, value);
	}
	
	public void setXAxisLabel(String topic, String value){
		this.xaxisLabels.put(topic, value);
	}
	
	public void setYAxisLabel(String topic, String value){
		this.yaxisLabels.put(topic, value);
	}
	
	public void initParameters() {
		Document doc = this.getDocument();
		ArrayList<String> categories = LabelNames.getCategories();
		ArrayList<String> topics = LabelNames.getTopics();
		final Element labels = (Element) doc.getElementsByTagName(LabelNames.LABELS.toString()).item(0);
		for(String category : categories){
			if(category.equalsIgnoreCase(LabelNames.TITLES.toString())){
				final Element titles = (Element) labels.getElementsByTagName(LabelNames.TITLES.toString()).item(0);
				for(String topic : topics){
					final NodeList topicTag = titles.getElementsByTagName(topic);
					this.setTitle(topic, topicTag.item(0).getTextContent());
				}
			}
			if(category.equalsIgnoreCase(LabelNames.XAXIS.toString())){
				final Element xaxis = (Element) labels.getElementsByTagName(LabelNames.XAXIS.toString()).item(0);
				for(String topic : topics){
					final NodeList topicTag = xaxis.getElementsByTagName(topic);
					this.setXAxisLabel(topic, topicTag.item(0).getTextContent());
				}
			}
			if(category.equalsIgnoreCase(LabelNames.YAXIS.toString())){
				final Element yaxis = (Element) labels.getElementsByTagName(LabelNames.YAXIS.toString()).item(0);
				for(String topic : topics){
					final NodeList topicTag = yaxis.getElementsByTagName(topic);
					this.setYAxisLabel(topic, topicTag.item(0).getTextContent());
				}
			}
		}
	}
}
