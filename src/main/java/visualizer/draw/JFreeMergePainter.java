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
import java.util.Set;
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

import visualizer.config.LabelNames;
import visualizer.config.LabelParser;
import visualizer.config.XmlConfigParser;
import visualizer.source.ISource;
import visualizer.structure.IStructure;

/**
 * @author Roland
 *
 */
public class JFreeMergePainter implements IPainter {

	private String benchName;
	private String variation;
	private String rootDirectory;
	private String chartDirectory;
	private String datasetDirectory;
	private ISource source;
	private XmlConfigParser configParser;
	private LabelParser labelParser;
	
	private static final String TOPOLOGY_INPUT = "topology_input";
	private static final String TOPOLOGY_THROUGHPUT = "topology_throughput";
	private static final String TOPOLOGY_DEPHASE = "topology_dephase";
	private static final String TOPOLOGY_LATENCY = "topology_latency";
	private static final String TOPOLOGY_NBEXEC = "topology_nb_executors";
	private static final String TOPOLOGY_NBSUPER = "topology_nb_supervisors";
	private static final String TOPOLOGY_NBWORK = "topology_nb_workers";
	//private static final String TOPOLOGY_STATUS = "topology_status";
	private static final String TOPOLOGY_TRAFFIC = "topology_traffic";
	//private static final String TOPOLOGY_REBALANCING = "topology_rebalancing";
	private static final String BOLT_INPUT = "bolt_input";
	private static final String BOLT_EXEC = "bolt_processed";
	private static final String BOLT_OUTPUT = "bolt_output";
	private static final String BOLT_LATENCY = "bolt_latency";
	//private static final String BOLT_CAPACITY = "bolt_capacity";
	//private static final String BOLT_ACTIVITY = "bolt_activity";
	private static final String BOLT_CPU = "bolt_cpu";
	private static final String BOLT_REBAL = "bolt_rebalancing";
	private static final String BOLT_PENDING = "bolt_pending";
	
	private static final String CAT_TOPOLOGY = "topology";
	private static final String CAT_BOLT = "bolts";
	
	private static final Logger logger = Logger.getLogger("JFreePainter");
	
	public JFreeMergePainter(String benchName, Integer varCode, ISource source, XmlConfigParser cp, LabelParser lp) {
		this.setBenchName(benchName);
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
		this.rootDirectory = this.getBenchName() + "_" + this.getVariation();
		this.chartDirectory = this.getRootDirectory() + "/charts";
		this.datasetDirectory = this.getRootDirectory() + "/datasets";
		
		Path root = Paths.get(this.getRootDirectory());
		Path chart = Paths.get(this.getChartDirectory());
		Path dataset = Paths.get(this.getDatasetDirectory());
		Path chartTopology = Paths.get(this.getChartDirectory() + "/" + CAT_TOPOLOGY);
		Path chartBolt = Paths.get(this.getChartDirectory() + "/" + CAT_BOLT);
		Path datasetTopology = Paths.get(this.getDatasetDirectory() + "/" + CAT_TOPOLOGY);
		Path datasetBolt = Paths.get(this.getDatasetDirectory() + "/" + CAT_BOLT);
		
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
		this.configParser = cp;
		this.labelParser = lp;
	}
	
	/**
	 * @return the benchName
	 */
	public String getBenchName() {
		return benchName;
	}
	/**
	 * @param benchName the benchName to set
	 */
	public void setBenchName(String benchName) {
		this.benchName = benchName;
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
	 * @param rootDirectory the rootDirectory to set
	 */
	public void setRootDirectory(String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}
	/**
	 * @return the chartDirectory
	 */
	public String getChartDirectory() {
		return chartDirectory;
	}
	/**
	 * @param chartDirectory the chartDirectory to set
	 */
	public void setChartDirectory(String chartDirectory) {
		this.chartDirectory = chartDirectory;
	}
	/**
	 * @return the datasetDirectory
	 */
	public String getDatasetDirectory() {
		return datasetDirectory;
	}
	/**
	 * @param datasetDirectory the datasetDirectory to set
	 */
	public void setDatasetDirectory(String datasetDirectory) {
		this.datasetDirectory = datasetDirectory;
	}
	/**
	 * @return the source
	 */
	public ISource getSource() {
		return source;
	}
	/**
	 * @param source the source to set
	 */
	public void setSource(ISource source) {
		this.source = source;
	}
	/**
	 * @return the configParser
	 */
	public XmlConfigParser getConfigParser() {
		return configParser;
	}
	/**
	 * @param configParser the configParser to set
	 */
	public void setConfigParser(XmlConfigParser configParser) {
		this.configParser = configParser;
	}
	/**
	 * @return the labelParser
	 */
	public LabelParser getLabelParser() {
		return labelParser;
	}
	/**
	 * @param labelParser the labelParser to set
	 */
	public void setLabelParser(LabelParser labelParser) {
		this.labelParser = labelParser;
	}
	
	public void drawXYSeries(XYSeriesCollection series, ArrayList<String> records, String seriesLabel, String category, String chartTitle, String xAxisLabel, String yAxisLabel, String... components){
		String component = "";
		if(components.length > 0){
			component = components[0];
		}
		Integer width = this.configParser.getWidth();
		Integer height = this.configParser.getHeight();
		String font = this.configParser.getFont();
		Integer titleFontSize = this.configParser.getTitleFontSize();
		Integer axisFontSize = this.configParser.getAxisFontSize();
		Integer legendFontSize = this.configParser.getLegendFontSize();
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(chartTitle, xAxisLabel, yAxisLabel, series, PlotOrientation.VERTICAL, true, true, true);
			Font fontAxis = new Font(font, Font.PLAIN, axisFontSize);
			Font fontTitle = new Font(font, Font.PLAIN, titleFontSize);
			Font fontLegend = new Font(font, Font.PLAIN, legendFontSize);
			
			xylineChart.getTitle().setFont(fontTitle);
			
			final XYPlot plot = xylineChart.getXYPlot();
			
			plot.setBackgroundPaint(Color.WHITE);
			
			plot.getDomainAxis().setLabelFont(fontAxis);
			plot.getRangeAxis().setLabelFont(fontAxis);
			
			ArrayList<Color> avgColors = new ArrayList<>();
			avgColors.add(Color.RED);
			avgColors.add(Color.BLUE);
			avgColors.add(Color.BLACK);
			avgColors.add(Color.MAGENTA);
			avgColors.add(Color.GREEN);
			int nbColors = avgColors.size();
			
			ArrayList<Color> minColors = new ArrayList<>();
			minColors.add(Color.ORANGE);
			minColors.add(Color.CYAN);
			minColors.add(Color.GRAY);
			minColors.add(Color.PINK);
			minColors.add(Color.getHSBColor(79, 66, 73));
			
			ArrayList<Color> maxColors = new ArrayList<>();
			maxColors.add(Color.RED);
			maxColors.add(Color.BLUE);
			maxColors.add(Color.BLACK);
			maxColors.add(Color.MAGENTA);
			maxColors.add(Color.GREEN);
			
			ArrayList<Shape> shapes = new ArrayList<>();
			shapes.add(new Rectangle2D.Double(0,0,5,5));
			shapes.add(new Ellipse2D.Double(0,0,5,5));
			shapes.add(ShapeUtilities.createDiagonalCross(3, 1));
			shapes.add(ShapeUtilities.createRegularCross(3, 1));
			shapes.add(ShapeUtilities.createDiamond(3));
			shapes.add(new Rectangle2D.Double(0,0,5,5));
			shapes.add(new Ellipse2D.Double(0,0,5,5));
			shapes.add(ShapeUtilities.createDiagonalCross(3, 1));
			int nbShapes = shapes.size();
			
			XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
			int nbSeries = series.getSeriesCount();
			HashMap<Integer, Color> serieColors = new HashMap<>();
			HashMap<Integer, Shape> serieShapes = new HashMap<>();
			HashMap<Integer, Boolean> serieLineVisible = new HashMap<>();
			HashMap<Integer, Boolean> serieShapeVisible = new HashMap<>();
			
			
			int i = 0;
			int serieSet = 0;
			while(i < nbSeries){
				String key = (String) series.getSeries(i).getKey();
				if(key.contains("_MIN")){
					renderer.setSeriesPaint(i, minColors.get(serieSet % nbColors));
					renderer.setSeriesShape(i, shapes.get(serieSet % nbShapes));
					renderer.setSeriesShapesVisible(i, true);
					renderer.setSeriesLinesVisible(i, false);
					serieColors.put(i, minColors.get(serieSet % nbColors));
					serieShapes.put(i, shapes.get(serieSet % nbShapes));
					serieShapeVisible.put(i, true);
					serieLineVisible.put(i, false);
				}
				if(key.contains("_MAX")){
					renderer.setSeriesPaint(i, maxColors.get(serieSet % nbColors));
					renderer.setSeriesShape(i, shapes.get(serieSet % nbShapes));
					renderer.setSeriesShapesVisible(i, true);
					renderer.setSeriesLinesVisible(i, false);
					serieColors.put(i, maxColors.get(serieSet % nbColors));
					serieShapes.put(i, shapes.get(serieSet % nbShapes));
					serieShapeVisible.put(i, true);
					serieLineVisible.put(i, false);
				}
				if(!key.contains("_MIN") && !key.contains("_MAX")){
					renderer.setSeriesPaint(i, avgColors.get(serieSet % nbColors));
					renderer.setSeriesShape(i, shapes.get(serieSet % nbShapes));
					renderer.setSeriesShapesVisible(i, false);
					renderer.setSeriesLinesVisible(i, true);
					serieColors.put(i, avgColors.get(serieSet % nbColors));
					serieShapes.put(i, shapes.get(serieSet % nbShapes));
					serieShapeVisible.put(i, false);
					serieLineVisible.put(i, true);
					serieSet++;
				}
				i++;
			}
			plot.setRenderer(renderer);
			
			LegendItemCollection legendItems = plot.getLegendItems();
			int n = legendItems.getItemCount();
			serieSet = 0;
			for(int j = 0; j < n; j++){
				LegendItem item = legendItems.get(j);
				item.setLinePaint(serieColors.get(j));
				item.setOutlinePaint(serieColors.get(j));
				item.setFillPaint(serieColors.get(j));
				item.setShape(serieShapes.get(j));
				item.setShapeVisible(serieShapeVisible.get(j));
				item.setLineVisible(serieLineVisible.get(j));
				item.setLabelFont(fontLegend);
			}
			plot.setFixedLegendItems(legendItems);
			
			File chart = new File(this.getChartDirectory() + "/" + category + "/" + component + "_" + seriesLabel + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		
		for(String topology : topologies){
			System.out.println("Topology: " + topology);
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}		
		}
		String title = this.labelParser.getTitle(LabelNames.INPUT.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.INPUT.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.INPUT.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_INPUT, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.TGHPT.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.TGHPT.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.TGHPT.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_THROUGHPUT, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.DEPH.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.DEPH.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.DEPH.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_DEPHASE, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.LATENCY.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.LATENCY.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.LATENCY.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_LATENCY, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);

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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.NBEXEC.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.NBEXEC.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.NBEXEC.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBEXEC, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.NBSUPER.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.NBSUPER.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.NBSUPER.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBSUPER, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.NBWORK.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.NBWORK.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.NBWORK.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_NBWORK, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyStatus()
	 */
	@Override
	public void drawTopologyStatus() {
		logger.warning("Deprecated method.");
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN");
			final XYSeries maxSerie = new XYSeries(topology + "_MAX");
			final XYSeries avgSerie = new XYSeries(topology);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.TRAFFIC.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.TRAFFIC.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.TRAFFIC.toString());
		drawXYSeries(dataToPlot, records, TOPOLOGY_TRAFFIC, CAT_TOPOLOGY, title, xAxisLabel, yAxisLabel);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyRebalancing(visualizer.structure.IStructure)
	 */
	@Override
	public void drawTopologyRebalancing(IStructure structure) {
		logger.warning("Deprecated method. It is recommended to use getBoltRebalancing instead");
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTIN.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTIN.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTIN.toString());
		drawXYSeries(dataToPlot, records, BOLT_INPUT, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTEXEC.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTEXEC.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTEXEC.toString());
		drawXYSeries(dataToPlot, records, BOLT_EXEC, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTOUT.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTOUT.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTOUT.toString());
		drawXYSeries(dataToPlot, records, BOLT_OUTPUT, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
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
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTLAT.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTLAT.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTLAT.toString());
		drawXYSeries(dataToPlot, records, BOLT_LATENCY, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltCapacity(java.lang.String)
	 */
	@Override
	public void drawBoltCapacity(String component) {
		logger.warning("Deprecated method.");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltActivity(java.lang.String)
	 */
	@Override
	public void drawBoltActivity(String component) {
		logger.warning("Deprecated method.");
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltCpuUsage(java.lang.String)
	 */
	@Override
	public void drawBoltCpuUsage(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltCpuUsage(component);
		ArrayList<String> records = new ArrayList<>();
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTCPU.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTCPU.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTCPU.toString());
		drawXYSeries(dataToPlot, records, BOLT_CPU, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltRebalancing(java.lang.String)
	 */
	@Override
	public void drawBoltRebalancing(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltRebalancing(component);
		ArrayList<String> records = new ArrayList<>();
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTREBAL.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTREBAL.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTREBAL.toString());
		drawXYSeries(dataToPlot, records, BOLT_REBAL, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltPending(java.lang.String)
	 */
	@Override
	public void drawBoltPending(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltPendings(component);
		ArrayList<String> records = new ArrayList<>();
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		ArrayList<String> topologies = new ArrayList<>();
		for(String topology : dataset.keySet()){
			if(!topology.endsWith("_MIN") && !topology.endsWith("_MAX")){
				topologies.add(topology);
			}
		}
		Collections.sort(topologies);
		for(String topology : topologies){
			HashMap<Integer, Double> minData = dataset.get(topology + "_MIN");
			HashMap<Integer, Double> maxData = dataset.get(topology + "_MAX");
			HashMap<Integer, Double> avgData = dataset.get(topology);
			final XYSeries minSerie = new XYSeries(topology + "_MIN" + "." + component);
			final XYSeries maxSerie = new XYSeries(topology + "_MAX" + "." + component);
			final XYSeries avgSerie = new XYSeries(topology + "." + component);
			records.add("timestamp;avg;min;max");
			Set<Integer> timestamps = avgData.keySet();
			for(Integer timestamp : timestamps){
				Double minValue = minData.get(timestamp);
				Double maxValue = maxData.get(timestamp);
				Double avgValue = avgData.get(timestamp);
				minSerie.add(timestamp, minValue);
				maxSerie.add(timestamp, maxValue);
				avgSerie.add(timestamp, avgValue);
				records.add(timestamp + ";" + avgValue + ";" + minValue + ";" + maxValue + ";");
			}
			boolean showAvg = this.getConfigParser().isShowAvg();
			boolean showMin = this.getConfigParser().isShowMin();
			boolean showMax = this.getConfigParser().isShowMax();
			
			if(showAvg){
				dataToPlot.addSeries(avgSerie);
			}
			if(showMin){
				dataToPlot.addSeries(minSerie);
			}
			if(showMax){
				dataToPlot.addSeries(maxSerie);			
			}	
		}
		String title = this.labelParser.getTitle(LabelNames.BOLTPEND.toString());
		String xAxisLabel = this.labelParser.getXAxisLabel(LabelNames.BOLTPEND.toString());
		String yAxisLabel = this.labelParser.getYAxisLabel(LabelNames.BOLTPEND.toString());
		drawXYSeries(dataToPlot, records, BOLT_PENDING, CAT_BOLT, title + " " + component, xAxisLabel, yAxisLabel, component);
	}

}