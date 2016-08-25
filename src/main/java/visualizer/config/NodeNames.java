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
	COMMAND("command"),
	TOPOLOGY1("topology1"),
	STREAMTYPE1("stream_type1"),
	TOPOLOGY2("topology2"),
	STREAMTYPE2("stream_type2"),
	DBHOST("db_host"),
	DBNAME("db_name"),
	DBUSER("db_user"),
	DBPWD("db_password"),
	EDGES1("edges1"),
	EDGES2("edges2"),
	EDGE1("edge1"),
	EDGE2("edge2"),
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
