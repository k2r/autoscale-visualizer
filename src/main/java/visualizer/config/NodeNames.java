/**
 * 
 */
package visualizer.config;

/**
 * @author Roland
 *
 */
public enum NodeNames {
	
	PARAM("parameters"),
	TOPOLOGY("topology"),
	STREAMTYPE("stream_type"),
	DBHOST("db_host"),
	DBNAME("db_name"),
	DBUSER("db_user"),
	DBPWD("db_password"),
	EDGES("edges"),
	EDGE("edge"),
	SOURCE("source"),
	DEST("destination");
	
	private String name = "";
	
	private NodeNames(String name) {
		this.name = name;
	}
	
	public String toString(){
		return this.name;
	}
}
