/**
 * 
 */
package visualizer.draw;

import java.awt.Color;
import java.awt.Font;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Logger;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.LegendItem;
import org.jfree.chart.LegendItemCollection;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.util.ShapeUtilities;

import visualizer.source.ISource;
import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public class JFreePainter implements IPainter {
	
	private String topology;
	private String variation;
	private String rootDirectory;
	private String chartDirectory;
	private String datasetDirectory;
	private ISource source;
	
	private static final String TOPOLOGY_INPUT = "topology_input";
	private static final String TOPOLOGY_THROUGHPUT = "topology_throughput";
	private static final String TOPOLOGY_DEPHASE = "topology_dephase";
	private static final String TOPOLOGY_LATENCY = "topology_latency";
	private static final String TOPOLOGY_NBEXEC = "topology_nb_executors";
	private static final String TOPOLOGY_NBSUPER = "topology_nb_supervisors";
	private static final String TOPOLOGY_NBWORK = "topology_nb_workers";
	private static final String TOPOLOGY_STATUS = "topology_status";
	private static final String TOPOLOGY_TRAFFIC = "topology_traffic";
	private static final String TOPOLOGY_REBALANCING = "topology_rebalancing";
	private static final String TOPOLOGY_LOAD = "topology_load";
	private static final String BOLT_INPUT = "bolt_input";
	private static final String BOLT_EXEC = "bolt_processed";
	private static final String BOLT_OUTPUT = "bolt_output";
	private static final String BOLT_LATENCY = "bolt_latency";
	private static final String BOLT_CAPACITY = "bolt_capacity";
	private static final String BOLT_ACTIVITY = "bolt_activity";
	private static final String BOLT_LOAD = "bolt_load";
	
	private static final String CAT_TOPOLOGY = "topology";
	private static final String CAT_BOLT = "bolts";
	private static final int CHART_WIDTH = 640;
	private static final int CHART_HEIGHT = 480;
	private static final boolean DRAWSHAPES = true;
	private static final boolean DRAWLINES = true;
	private static final Integer TITLE_FONTSIZE = 28; 
	private static final Integer AXIS_FONTSIZE = 24;
	private static final Integer LEGEND_FONTSIZE = 14;
	
	private static final Logger logger = Logger.getLogger("JFreePainter");
	
	public JFreePainter(String topology, Integer varCode, ISource source) {
		this.setTopology(topology);
		switch(varCode){
		case(1): this.setVariation("linear_increase");
				break;
		case(2): this.setVariation("scale_increase");
				break;
		case(3): this.setVariation("exponential_increase");
				break;
		case(4): this.setVariation("logarithmic_increase");
				break;
		case(5): this.setVariation("linear_decrease");
				break;
		case(6): this.setVariation("scale_decrease");
				break;
		case(7): this.setVariation("exponential_decrease");
				break;
		case(8): this.setVariation("all_variations");
				break;
		case(9): this.setVariation("no_variation");
				break;
		}
		this.rootDirectory = this.getTopology() + "_" + this.getVariation();
		this.chartDirectory = this.getRootDirectory() + "/charts";
		this.datasetDirectory = this.getRootDirectory() + "/datasets";
		
		Path root = Paths.get(this.getRootDirectory());
		Path chart = Paths.get(this.getChartDirectory());
		Path dataset = Paths.get(this.getDatasetDirectory());
		Path chartTopology = Paths.get(this.getChartDirectory() + "/topology");
		Path chartBolt = Paths.get(this.getChartDirectory() + "/bolts");
		Path datasetTopology = Paths.get(this.getDatasetDirectory() + "/topology");
		Path datasetBolt = Paths.get(this.getDatasetDirectory() + "/bolts");
		
		try {
			if(!Files.exists(root)){
				Files.createDirectory(root);
				Files.createDirectory(chart);
				Files.createDirectory(dataset);
				Files.createDirectory(chartTopology);
				Files.createDirectory(chartBolt);
				Files.createDirectory(datasetTopology);
				Files.createDirectory(datasetBolt);
			}
		} catch (IOException e) {
			logger.severe("Unable to initialize the painter because " + e);
		}
		this.source = source;
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
	 * @return the variation
	 */
	public String getVariation() {
		return variation;
	}

	/**
	 * @param variation the variation to set
	 */
	public void setVariation(String variation) {
		this.variation = variation;
	}

	/**
	 * @return the rootDirectory
	 */
	public String getRootDirectory() {
		return rootDirectory;
	}

	/**
	 * @return the chartDirectory
	 */
	public String getChartDirectory() {
		return chartDirectory;
	}

	/**
	 * @return the datasetDirectory
	 */
	public String getDatasetDirectory() {
		return datasetDirectory;
	}
	
	public void drawXYSeries(XYSeriesCollection series, ArrayList<String> records, String seriesLabel, String category, String chartTitle, String xAxisLabel, String yAxisLabel, String... components){
		String component = "";
		if(components.length > 0){
			component = components[0];
		}
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(chartTitle, xAxisLabel, yAxisLabel, series, PlotOrientation.VERTICAL, true, true, true);
			Font fontAxis = new Font("Dialog", Font.PLAIN, AXIS_FONTSIZE);
			Font fontTitle = new Font("Dialog", Font.PLAIN, TITLE_FONTSIZE);
			Font fontLegend = new Font("Dialog", Font.PLAIN, LEGEND_FONTSIZE);
			
			xylineChart.getTitle().setFont(fontTitle);
			
			final XYPlot plot = xylineChart.getXYPlot();
			
			plot.setBackgroundPaint(Color.WHITE);
			
			plot.getDomainAxis().setLabelFont(fontAxis);
			plot.getRangeAxis().setLabelFont(fontAxis);
			
			ArrayList<Color> colors = new ArrayList<>();
			colors.add(Color.RED);
			colors.add(Color.BLUE);
			colors.add(Color.GRAY);
			colors.add(Color.BLACK);
			colors.add(Color.GREEN);
			colors.add(Color.PINK);
			colors.add(Color.CYAN);
			colors.add(Color.ORANGE);
			
			ArrayList<Shape> shapes = new ArrayList<>();
			shapes.add(new Rectangle2D.Double(0,0,5,5));
			shapes.add(new Ellipse2D.Double(0,0,5,5));
			shapes.add(ShapeUtilities.createDiagonalCross(3, 1));
			shapes.add(ShapeUtilities.createRegularCross(3, 1));
			shapes.add(ShapeUtilities.createDiamond(3));
			shapes.add(new Rectangle2D.Double(0,0,5,5));
			shapes.add(new Ellipse2D.Double(0,0,5,5));
			shapes.add(ShapeUtilities.createDiagonalCross(3, 1));
			
			XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
			int nbSeries = series.getSeriesCount();
			for(int i = 0; i < nbSeries; i++){
				renderer.setSeriesPaint(i, colors.get(i));
				renderer.setSeriesShape(i, shapes.get(i));
				renderer.setSeriesShapesVisible(i, DRAWSHAPES);
				renderer.setSeriesLinesVisible(i, DRAWLINES);
			}
			plot.setRenderer(renderer);
			
			LegendItemCollection legendItems = plot.getLegendItems();
			int n = legendItems.getItemCount();
			for(int i = 0; i < n; i++){
				LegendItem item = legendItems.get(i);
				int serieIndex = item.getSeriesIndex();
				item.setLinePaint(colors.get(serieIndex));
				item.setOutlinePaint(colors.get(serieIndex));
				item.setFillPaint(colors.get(serieIndex));
				item.setShape(shapes.get(serieIndex));
				item.setShapeVisible(DRAWSHAPES);
				item.setLabelFont(fontLegend);
			}
			plot.setFixedLegendItems(legendItems);
			
			File chart = new File(this.getChartDirectory() + "/" + category + "/" + component + "_" + seriesLabel + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, CHART_WIDTH, CHART_HEIGHT);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + seriesLabel + " because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/" + category + "/" + component + "_" + seriesLabel + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save " + seriesLabel + " because " + e);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyInput()
	 */
	@Override
	public void drawTopologyInput() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyInput();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;input");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}	
		drawXYSeries(dataToPlot, records, TOPOLOGY_INPUT, CAT_TOPOLOGY, "Flux d'entrée", "timestamp (en s)", "Nombre de n-uplets émis");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyThroughput()
	 */
	@Override
	public void drawTopologyThroughput() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyThroughput();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;throughput");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}	
		drawXYSeries(dataToPlot, records, TOPOLOGY_THROUGHPUT, CAT_TOPOLOGY, "Débit en sortie", "timestamp (en s)", "Nombre de n-uplets en sortie");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyLosses()
	 */
	@Override
	public void drawTopologyLosses() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyDephase();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;losses");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_DEPHASE, CAT_TOPOLOGY, "N-uplets déphasés", "timestamp (en s)", "Nombre de n-uplets déphasés");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyLatency()
	 */
	@Override
	public void drawTopologyLatency() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyLatency();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;throughput");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_LATENCY, CAT_TOPOLOGY, "Latence de la topologie", "timestamp (en s)", "Latence moyenne par n-uplet (en ms)");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbExecutors()
	 */
	@Override
	public void drawTopologyNbExecutors() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbExecutors();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;nbExecutors");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBEXEC, CAT_TOPOLOGY, "Nombre d'executors", "timestamp (en s)", "Nombre d'executors");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbSupervisors()
	 */
	@Override
	public void drawTopologyNbSupervisors() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbSupervisors();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;nbSupervisors");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBSUPER, CAT_TOPOLOGY, "Nombre de Supervisors", "timestamp (en s)", "Nombre de supervisors");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbWorkers()
	 */
	@Override
	public void drawTopologyNbWorkers() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbWorkers();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;nbWorkers");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBWORK, CAT_TOPOLOGY, "Nombre de workers", "timestamp (en s)", "Nombre de workers");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyStatus()
	 */
	@Override
	public void drawTopologyStatus() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyStatus();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;status");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_STATUS, CAT_TOPOLOGY, "État de la topologie", "timestamp (en s)", "État");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public void drawTopologyTraffic(IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyTraffic(structure);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;traffic");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_TRAFFIC, CAT_TOPOLOGY, "Trafic réseau", "timestamp (en s)", "Trafic (en n-uplets)");
	}

	@Override
	public void drawTopologyRebalancing(IStructure structure) {
		HashMap<String, HashMap<String, HashMap<Integer, Double>>> dataset = this.source.getTopologyRebalancing(structure);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<String, HashMap<Integer, Double>> boltData = dataset.get(topology);
			
			records.add("timestamp;actions");
			for(String bolt : boltData.keySet()){
				XYSeries serie = new XYSeries(topology + "." + bolt);
				HashMap<Integer, Double> actions = boltData.get(bolt);
				for(Integer timestamp : actions.keySet()){
					Double nbExecutors = actions.get(timestamp);
					serie.add(timestamp, nbExecutors);
					String boltExecutors = bolt + "@" + nbExecutors + ":";
					records.add(timestamp + ";" + boltExecutors);
				}
				dataToPlot.addSeries(serie);
			}
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_REBALANCING, CAT_TOPOLOGY, "Modification des degrés de parallélisme", "timestamp (en s)", "Nombre d'executors");
	}
	
	@Override
	public void drawTopologyLoad() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyLoads();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology);
			records.add("timestamp;loads");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, TOPOLOGY_LOAD, CAT_TOPOLOGY, "Charge moyenne de la topologie", "timestamp (en s)", "Charge");
	}
	
	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public void drawBoltInput(String component, IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltInput(component, structure);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;input");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_INPUT, CAT_BOLT, "Flux d'entrée du bolt " + component, "timestamp (en s)", "Nombre de n-uplets", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltExecuted(java.lang.String)
	 */
	@Override
	public void drawBoltExecuted(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltExecuted(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;executed");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_EXEC, CAT_BOLT, "N-uplets traités par le bolt " + component, "timestamp (en s)", "Nombre de n-uplets", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltOutputs(java.lang.String)
	 */
	@Override
	public void drawBoltOutputs(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltOutputs(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;outputs");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_OUTPUT, CAT_BOLT, "Flux en sortie du bolt " + component, "timestamp (en s)", "Nombre de n-uplets", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltLatency(java.lang.String)
	 */
	@Override
	public void drawBoltLatency(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltLatency(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;latency");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_LATENCY, CAT_BOLT, "Latence du bolt " + component, "timestamp (en s)", "Latence moyenne par n-uplet (en ms)", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltProcRate(java.lang.String)
	 */
	@Override
	public void drawBoltCapacity(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltProcessingRate(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;processing_rate");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_CAPACITY, CAT_BOLT, "Capacité de traitement du bolt " + component, "timestamp (en s)", "Capacité moyenne (en n-uplets/s)", component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltEPR(java.lang.String)
	 */
	@Override
	public void drawBoltActivity(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltCR(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;cr");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_ACTIVITY, CAT_BOLT, "Niveau d'activité du bolt " + component, "timestamp (en s)", "Niveau d'activité", component);
	}
	
	@Override
	public void drawBoltLoad(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltPL(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			topologies.add(topology);
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;pl");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		drawXYSeries(dataToPlot, records, BOLT_LOAD, CAT_BOLT, "Charge du bolt " + component, "timestamp (en s)", "Charge", component);
	}
}