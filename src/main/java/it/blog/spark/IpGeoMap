package it.blog.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class IpGeoMap {

	final static Logger logger = Logger.getLogger(IpGeoMap.class);
	/*
	 * MaxMind Lookup
	 */
	private static LookupService lookupService;
	
	/*
	 * Application properties
	 */
	private static Properties prop;
	
	private static final Function<String, Boolean> LOG_FILTER = new Function<String, Boolean>()
	{

		@Override
		public Boolean call(String v1) throws Exception {
			if (v1.equals("not valid"))
				return false;
			else
				return true;
		}

	};
			

	private static final PairFunction<String, String, String> LOG_MAPPER = new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) throws Exception {

			return new Tuple2<String, String>(s.trim(), s.trim());
		}
	};

	private static final Function2<String, String, String> LOG_REDUCER = new Function2<String, String, String>() {

		public String call(String a, String b) throws Exception {
			return a;
		}
	};

	private static final Function<Tuple2<String, String>, GeoLocation> MAXMIND_MAPPER = new Function<Tuple2<String, String>, GeoLocation>() {

		@Override
		public GeoLocation call(Tuple2<String, String> v1) throws Exception {

			Location location = lookupService.getLocation(v1._1);

			GeoLocation geo = new GeoLocation();
			geo.setLatitude(location.latitude);
			geo.setLongitude(location.longitude);

			return geo;
		}
	};

	/**
	 * Initialize Application
	 */
	private static void init(String configFile)
	{
		prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream(configFile);

			// load a properties file
			prop.load(input);
			

		} catch (IOException ex) {
			logger.error("Error loading config file", ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
		}
		
		init(args[0]);

		lookupService = new LookupService(prop.getProperty("file.geoipcity"));

		/*
		 * Only for Windows
		 */
		System.setProperty("hadoop.home.dir", prop.getProperty("hadoop.home.dir"));

		/*
		 * Configure the application
		 */
		SparkConf conf = new SparkConf().setAppName("org.sparkexample.IpGeoMap").setMaster("local");
		
		/*
		 * Start the context
		 */
		JavaSparkContext context = new JavaSparkContext(conf);
		
		/*
		 * Load the log
		 */
		JavaRDD<String> logLines = context.textFile(prop.getProperty("file.log"));
		
		/*
		 * Extract the Ip Address
		 */
		JavaRDD<String> ipFromLog = logLines.map(ParseLog::parseFromLogLine);
		
		
		/*
		 * Filter the valid entry and Map them.
		 */
		JavaPairRDD<String, String> pairs = ipFromLog.filter(LOG_FILTER).mapToPair(LOG_MAPPER);
		
		/*
		 * Remove the duplicate entries
		 */
		JavaPairRDD<String, String> reducer = pairs.reduceByKey(LOG_REDUCER);		
		
		/*
		 * Produce the geo location 
		 */
		JavaRDD<GeoLocation> maxMind = reducer.map(MAXMIND_MAPPER);

		/*
		 * Write the output file
		 */
		writeFile(produceJson(maxMind));

		context.stop();
		context.close();
	}

	/**
	 * Produce Output Json 
	 * @param maxMind
	 * @return
	 * @throws JsonProcessingException
	 */
	private static String produceJson(JavaRDD<GeoLocation> maxMind) throws JsonProcessingException
	{
		ArrayList<String> text = new ArrayList<String>();
		ObjectMapper mapper = new ObjectMapper();
		Iterator<GeoLocation> geoIterator = maxMind.toLocalIterator();

		while (geoIterator.hasNext()) {
			GeoLocation person = geoIterator.next();
			text.add(mapper.writeValueAsString(person));
		}
		
		return text.toString();
	}
	
	/**
	 * Write Output file
	 * @param text
	 * @throws IOException
	 */
	private static void writeFile(String text) throws IOException {
		File logFile = new File(prop.getProperty("file.output"));

		BufferedWriter writer = new BufferedWriter(new FileWriter(logFile));

		writer.write(text);

		writer.close();
	}
	
}
