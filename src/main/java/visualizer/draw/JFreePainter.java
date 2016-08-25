/**
 * 
 */
package visualizer.draw;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

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
	private static final String TOPOLOGY_LOSSES = "topology_losses";
	private static final String TOPOLOGY_LATENCY = "topology_latency";
	private static final String TOPOLOGY_NBEXEC = "topology_nb_executors";
	private static final String TOPOLOGY_NBSUPER = "topology_nb_supervisors";
	private static final String TOPOLOGY_NBWORK = "topology_nb_workers";
	private static final String TOPOLOGY_STATUS = "topology_status";
	private static final String TOPOLOGY_TRAFFIC = "topology_traffic";
	private static final String BOLT_INPUT = "bolt_input";
	private static final String BOLT_EXEC = "bolt_processed";
	private static final String BOLT_OUTPUT = "bolt_output";
	private static final String BOLT_LATENCY = "bolt_latency";
	private static final String BOLT_PROCRATE = "bolt_processing_rate";
	private static final String BOLT_EPR = "bolt_epr";
	
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
	
	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyInput()
	 */
	@Override
	public void drawTopologyInput() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyInput();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " input stream", "timestamp (in s)", "Number of incoming tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_INPUT + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology input because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_INPUT + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology input because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyThroughput()
	 */
	@Override
	public void drawTopologyThroughput() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyThroughput();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " thoughput", "timestamp (in s)", "Number of outcoming tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_THROUGHPUT + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology throughput because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_THROUGHPUT + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology thoughput because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyLosses()
	 */
	@Override
	public void drawTopologyLosses() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyLosses();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " losses", "timestamp (in s)", "Number of replayed tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_LOSSES + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology losses because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_LOSSES + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology losses because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyLatency()
	 */
	@Override
	public void drawTopologyLatency() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyLatency();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " latency", "timestamp (in s)", "Average complete latency (in ms)", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_LATENCY + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology latency because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_LATENCY + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology latency because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbExecutors()
	 */
	@Override
	public void drawTopologyNbExecutors() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbExecutors();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " number of executors", "timestamp (in s)", "Number of executors", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_NBEXEC + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology executors because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_NBEXEC + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology executors because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbSupervisors()
	 */
	@Override
	public void drawTopologyNbSupervisors() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbSupervisors();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " number of supervisors", "timestamp (in s)", "Number of supervisors", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_NBSUPER + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology supervisors because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_NBSUPER + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology supervisors because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyNbWorkers()
	 */
	@Override
	public void drawTopologyNbWorkers() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyNbWorkers();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " number of workers", "timestamp (in s)", "Number of workers", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_NBWORK + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology workers because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_NBWORK + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology workers because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyStatus()
	 */
	@Override
	public void drawTopologyStatus() {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyStatus();
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " status", "timestamp (in s)", "Topology status", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_STATUS + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology status because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_STATUS + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology status because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawTopologyTraffic(visualizer.structure.IStructure)
	 */
	@Override
	public void drawTopologyTraffic(IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getTopologyTraffic(structure);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + " network traffic", "timestamp (in s)", "Network traffic (in tuples)", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory() + "/topology/" + TOPOLOGY_TRAFFIC + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of the topology traffic because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory() + "/topology/" + TOPOLOGY_TRAFFIC + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the topology traffic because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltInput(java.lang.String, visualizer.structure.IStructure)
	 */
	@Override
	public void drawBoltInput(String component, IStructure structure) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltInput(component, structure);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " inputs", "timestamp (in s)", "Number of incoming tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_INPUT + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " inputs because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_INPUT + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " inputs because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltExecuted(java.lang.String)
	 */
	@Override
	public void drawBoltExecuted(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltExecuted(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " processed", "timestamp (in s)", "Number of processed tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_EXEC + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " executed tuples because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_EXEC + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " executed tuples because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltOutputs(java.lang.String)
	 */
	@Override
	public void drawBoltOutputs(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltOutputs(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " outputs", "timestamp (in s)", "Number of emitted tuples", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_OUTPUT + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " emitted tuples because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_OUTPUT + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " emitted tuples because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltLatency(java.lang.String)
	 */
	@Override
	public void drawBoltLatency(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltLatency(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " latency", "timestamp (in s)", "Average latency per tuple (in ms)", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_LATENCY + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " latency because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_LATENCY + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " latency because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltProcRate(java.lang.String)
	 */
	@Override
	public void drawBoltProcRate(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltProcessingRate(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
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
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " processing rate", "timestamp (in s)", "Average processing rate (in tuples per window)", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_PROCRATE + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " processing rate because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_PROCRATE + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " processing rate because " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see visualizer.draw.IPainter#drawBoltEPR(java.lang.String)
	 */
	@Override
	public void drawBoltEPR(String component) {
		HashMap<String, HashMap<Integer, Double>> dataset = this.source.getBoltEPR(component);
		ArrayList<String> records = new ArrayList<>();
	
		final XYSeriesCollection dataToPlot = new XYSeriesCollection();
		
		for(String topology : dataset.keySet()){
			HashMap<Integer, Double> data = dataset.get(topology);
			final XYSeries serie = new XYSeries(topology + "." + component);
			records.add("timestamp;epr");
			for(Integer timestamp : data.keySet()){
				Double value = data.get(timestamp);
				records.add(timestamp + ";" + value);
				serie.add(timestamp, value);
			}
			dataToPlot.addSeries(serie);
		}
		
		if(Files.exists(Paths.get(this.getChartDirectory()))){
			JFreeChart xylineChart = ChartFactory.createXYLineChart(this.getTopology() + "." + component + " epr", "timestamp (in s)", "epr", dataToPlot, PlotOrientation.VERTICAL, true, true, true);
			int width = 640;
			int height = 480;
			File chart = new File(this.getChartDirectory()  + "/bolts/" + component + "_" + BOLT_EPR + "_" + this.getRootDirectory() + ".png");
			try {
				ChartUtilities.saveChartAsPNG(chart, xylineChart, width, height);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " epr because " + e);
			}
		}
		if(Files.exists(Paths.get(this.getDatasetDirectory()))){
			try {
				Path path = Paths.get(this.getDatasetDirectory()  + "/bolts/" + component + "_" + BOLT_EPR + "_" + this.getRootDirectory() + ".csv");
				Files.createFile(path);
				Files.write(path, records, Charset.defaultCharset(), StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.severe("Unable to save the chart of " + component + " epr because " + e);
			}
		}
	}
}