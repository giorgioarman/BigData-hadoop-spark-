package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		//Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		JavaRDD<String> clearInputRDD = inputRDD.filter(f -> ! f.startsWith("id"));
		
		JavaPairRDD<String, String> UserProductPairRDD = clearInputRDD.mapToPair(f -> {
			// field[1] productId 
			//field[2]  userId 
			String[] fields = f.split(",");
			
			Tuple2<String,String> result= new Tuple2 <String ,String> (fields[2],fields[1]);
			return result;			
		}); 
		
		JavaPairRDD<String,Iterable<String>> UserListProductRDD = UserProductPairRDD.groupByKey();
		
		JavaRDD<Iterable<String>> PairsProductValuesRDD = UserListProductRDD.values();
		
		
		JavaPairRDD<String,Integer> PairsProductRDD = PairsProductValuesRDD.flatMapToPair(f -> {
			
			
			List<Tuple2<String,Integer>> products = new ArrayList<Tuple2<String,Integer>>();			
			
			for (String x : f) {
				for (String y : f) {					
						if (x.compareTo(y) > 0)
							products.add(new Tuple2 <String,Integer> (x+ "_"+y ,1));	
				}
			}
			
			return products.iterator();
			
		});
		
		JavaPairRDD<String, Integer> FrequenciesPairProductRDD = PairsProductRDD.reduceByKey((a, b) -> a +b);
		
		
		JavaPairRDD<Integer, String> PairProductFrequenciesRDD = FrequenciesPairProductRDD
				.mapToPair(f -> new Tuple2<Integer,String>(f._2,f._1));
		
		
		JavaPairRDD<Integer, String> PairProductFrequenciesSortedRDD = PairProductFrequenciesRDD.sortByKey(false);	
		
		List<Tuple2<Integer,String>> top = PairProductFrequenciesSortedRDD.top(10, new FrequencyComparator());	
		
		JavaPairRDD<Integer,String> Top10RDD = sc.parallelizePairs(top);
		
		// Store the result in the output folder
		Top10RDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}


