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
	TOPOLOGY("topology"),
	SHTOPOLOGY("short_topology"),
	LGDTOPOLOGY("legend_name"),
	STREAMTYPE("stream_type"),
	DBHOST("db_host"),
	DBNAME("db_name"),
	DBUSER("db_user"),
	DBPWD("db_password"),
	EDGES("edges"),
	EDGE("edge"),
	SOURCE("source"),
	DEST("destination"),
	LANG("language"),
	DRAWSHP("draw_shapes"),
	DRAWLN("draw_lines"),
	WIDTH("width"),
	HEIGHT("height"),
	FONT("font"),
	TTLSIZE("title_fontsize"),
	AXSIZE("axis_fontsize"),
	TICKSIZE("tick_fontsize"),
	LGDSIZE("legend_fontsize"),
	SHOWAVG("show_average"),
	SHOWMIN("show_min"),
	SHOWMAX("show_max"),
	MINTIME("min_timestamp"),
	MAXTIME("max_timestamp");
	
	private String name = "";
	
	private NodeNames(String name) {
		this.name = name;
	}
	
	public String toString(){
		return this.name;
	}
}
