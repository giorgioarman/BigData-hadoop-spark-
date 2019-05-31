package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

import scala.reflect.internal.Trees.Return;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = new Double(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();
		
		Dataset<Row> readingInputRow = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath);
		
		Dataset<ReadingInput> readingInput = readingInputRow.as(Encoders.bean(ReadingInput.class));
		
		Dataset<ReadingInput> filteredData = readingInput.filter(r -> {
			if(r.getFreeSlot() == 0 && r.getUsedSlot() == 0)
				return false;
			else 
				return true;						
		});
		
		
		Dataset<StationAllData> stationAllData = filteredData.map(r -> {
			StationAllData newRow = new StationAllData();
			newRow.setStationID(r.getStationId());
			newRow.setDayOfWeek(DateTool.DayOfTheWeek(r.getTimeStamp()));
			newRow.setHour(DateTool.hour(r.getTimeStamp()));			
			// state = 1     critical 
			// state = 0     not critical 
			
			Double state;
			if(r.getFreeSlot() == 0)
				state = (double) 1;
			else
				state = (double) 0;
			newRow.setState(state);
			
			return newRow;
		}, Encoders.bean(StationAllData.class));           
		
		RelationalGroupedDataset rgd = stationAllData.groupBy("stationID","dayOfWeek","hour");
		
		Dataset<Row> stationDataCriticalRow = rgd.agg(avg("state"));
		
		Dataset<StationAllData> stationDataCritical = stationDataCriticalRow
				.withColumnRenamed("avg(state)", "state").as(Encoders.bean(StationAllData.class));
		
		
		
		Dataset<StationAllData> stationsGreaterThanTreshold = stationDataCritical.filter(r ->{
			if (r.getState() > threshold)
				return true;
			else 
				return false;
		});
		
		Dataset<Row> readingStationDataRow = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath2);
		
		Dataset<StationReading> readingStationData =
				readingStationDataRow.as(Encoders.bean(StationReading.class));
		
		
		Dataset<Row> joinedTable = stationsGreaterThanTreshold.join(readingStationData,
				stationsGreaterThanTreshold.col("stationID").equalTo(readingStationData.col("stationID")));
		
			
		joinedTable.write().format("csv")
		.option("header", true).save(outputFolder);
		
		// Close the Spark session
		ss.stop();
	}
}
