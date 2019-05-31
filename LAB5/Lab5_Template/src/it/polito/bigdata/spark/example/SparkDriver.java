package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		String startWord;
		
		inputPath=args[0];
		outputPath=args[1];
		startWord=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);
		
		

		// Filter the rows that start with 'startWord'
		JavaRDD<String> resultRDD = logRDD.filter(line -> line.startsWith(startWord));	
		long lineCount = resultRDD.count();
		
		JavaRDD<Integer> wordCount = resultRDD.map(line ->{
			String[] rowSplit = line.split("\t");
			Integer wordCounted = Integer.parseInt(rowSplit[1]);
			return wordCounted;
		});  
		
		Integer maxFreq = wordCount.reduce((value1,value2) -> {
			if (value1> value2)
				return value1;
			else 
				return value2;
		});
		
		
		System.out.println("The number of line is : " + lineCount + " .");
		System.out.println("The maximum is : " + maxFreq + " .");
		
		
		JavaRDD<String> resultGreatRDD = resultRDD.filter(line -> {
			String[] rowSplit = line.split("\t");
			Integer wordCounted = Integer.parseInt(rowSplit[1]);		
			
			if (wordCounted > (0.8 * maxFreq)) {
				return true;
			} else {
				return false;
			}	
						
		});
		long lineCountg = resultGreatRDD.count();
		System.out.println("The number of line greater than 80% of max is: " + lineCountg + " .");
		
		
		
		// Store the result in the output folder
		resultGreatRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
