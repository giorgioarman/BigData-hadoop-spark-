package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputNameFileKML;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = new Double(args[2]);
		outputNameFileKML = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		JavaRDD<String> FilteredData = inputRDD.filter(f ->{
			if (f.startsWith("s"))
				return false;
			else 
			{
				String[] fields = f.split("\t");
				if (Integer.parseInt( fields[2]) == 0 && Integer.parseInt(fields[3]) == 0)
					return false;
				else 
					return true;
			}
		});
		
		JavaPairRDD<String, Count> stationCountPairRDD = FilteredData.mapToPair(line -> {
			String[] fields = line.split("\t");
			
			// fields[0] is the station id
			// fields[1] is date and time 
			// fields[2] is used slots
			// fields[3] is the number of free slots
			
			DateTool ok = new DateTool();
			
			String[] DateTime = fields[1].split(" ");
			String DayOfWeek = ok.DayOfTheWeek(DateTime[0]);
			
			String[] timeSlot = DateTime[1].split(":");			
			
			
			
			String KeyString = fields[0] + "_" + DayOfWeek + "_" + timeSlot[0] ;


			if (Integer.parseInt(fields[3]) == 0 )
				return new Tuple2<String, Count>(KeyString, new Count(1, 1));
			else
				return new Tuple2<String, Count>(KeyString, new Count(1, 0));
		});
		
				
		JavaPairRDD<String, Count> stationTotalCountPairRDD = stationCountPairRDD.reduceByKey(new Sum());
		
		
		JavaPairRDD <String, Double> CriticalValueStastionsPairRDD = stationTotalCountPairRDD.mapToPair(f -> {
			
			
			
			Double criticalValue = (double)f._2.numCriticalReadings / (double)f._2.numReadings;
			
			return new Tuple2<String, Double>(f._1 , criticalValue);
			
		});
		
		
		JavaPairRDD <String, Double> CriticalGreaterThresholdStastionsPairRDD = CriticalValueStastionsPairRDD.filter(f -> {
			if (f._2 > threshold)
				return true ;
			else 
				return false;
		});
		
		
		
		

		// TODO
		// .....
		// .....
		// .....

		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML = ;
		JavaRDD<String> resultKML = null;
		
		// There is at most one string for each station. We can use collect and
		// store the returned list in the main memory of the driver.
		List<String> localKML = resultKML.collect();
		
		// Store the result in one single file stored in the distributed file
		// system
		// Add header and footer, and the content of localKML in the middle
		Configuration confHadoop = new Configuration();

		try {
			URI uri = URI.create(outputNameFileKML);

			FileSystem file = FileSystem.get(uri, confHadoop);
			FSDataOutputStream outputFile = file.create(new Path(uri));

			BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

			// Header
			bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
			bOutFile.newLine();

			// Markers
			for (String lineKML : localKML) {
				bOutFile.write(lineKML);
				bOutFile.newLine();
			}

			// Footer
			bOutFile.write("</Document></kml>");
			bOutFile.newLine();

			bOutFile.close();
			outputFile.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Close the Spark context
		sc.close();
	}
}
