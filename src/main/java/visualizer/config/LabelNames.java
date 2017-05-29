package visualizer.config;

import java.util.ArrayList;

public enum LabelNames {

	LABELS("labels"),
	TITLES("titles"),
	XAXIS("xaxis"),
	YAXIS("yaxis"),
	INPUT("input"),
	TGHPT("throughput"),
	DEPH("dephase"),
	LATENCY("latency"),
	NBEXEC("nb_executor"),
	NBSUPER("nb_supervisor"),
	NBWORK("nb_worker"),
	STATUS("status"),
	TRAFFIC("traffic"),
	REBAL("rebalancing"),
	BOLTIN("bolt_input"),
	BOLTEXEC("bolt_exec"),
	BOLTOUT("bolt_output"),
	BOLTLAT("bolt_latency"),
	BOLTCAP("bolt_capacity"),
	BOLTACT("bolt_activity"),
	BOLTCPU("bolt_cpu"),
	BOLTREBAL("bolt_rebalancing");
	
	private String name = "";
	
	private LabelNames(String name) {
		this.name = name;
	}
	
	public static ArrayList<String> getCategories(){
		ArrayList<String> categories = new ArrayList<>();
	
		categories.add(LabelNames.TITLES.toString());
		categories.add(LabelNames.XAXIS.toString());
		categories.add(LabelNames.YAXIS.toString());
	
		return categories;
	}
	
	public static ArrayList<String> getTopics(){
		ArrayList<String> topics = new ArrayList<>();
		
		topics.add(LabelNames.INPUT.toString());
		topics.add(LabelNames.TGHPT.toString());
		topics.add(LabelNames.DEPH.toString());
		topics.add(LabelNames.LATENCY.toString());
		topics.add(LabelNames.NBEXEC.toString());
		topics.add(LabelNames.NBSUPER.toString());
		topics.add(LabelNames.NBWORK.toString());
		topics.add(LabelNames.STATUS.toString());
		topics.add(LabelNames.TRAFFIC.toString());
		topics.add(LabelNames.REBAL.toString());
		topics.add(LabelNames.BOLTIN.toString());
		topics.add(LabelNames.BOLTEXEC.toString());
		topics.add(LabelNames.BOLTOUT.toString());
		topics.add(LabelNames.BOLTLAT.toString());
		topics.add(LabelNames.BOLTCAP.toString());
		topics.add(LabelNames.BOLTACT.toString());
		topics.add(LabelNames.BOLTCPU.toString());
		topics.add(LabelNames.BOLTREBAL.toString());
		return topics;
	}
	
	public String toString(){
		return this.name;
	}
}