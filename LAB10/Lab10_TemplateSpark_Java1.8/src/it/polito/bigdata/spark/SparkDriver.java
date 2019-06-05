package it.polito.bigdata.spark;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
	
public class SparkDriver {
	
	
	public static void main(String[] args) throws InterruptedException {

		
		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Streaming Lab 10");
				
		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));		
		
		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");

		JavaDStream<String> tweets = jssc.textFileStream(inputFolder);

		// TODO
		// Process the tweets JavaDStream.
		// Every time a new file is uploaded  in inputFolder a new set of streaming data 
		// is generated 
		// ...
		
		JavaPairDStream<String, Integer> hashtags = tweets.flatMapToPair(tweet -> {
			String[] fields = tweet.split("\t");
			ArrayList<Tuple2<String,Integer>> hashTagsInTweet = new ArrayList<>();
			
			
			String[] wordsInTweet = fields[1].split("\\s+");
			
			for (String word : wordsInTweet) {
				if(word.startsWith("#")){
					word = word.toLowerCase();
					Tuple2<String, Integer> wordHashtag = new Tuple2 <String ,Integer> (word, 1);
					hashTagsInTweet.add(wordHashtag);
				}
					
			}
			return hashTagsInTweet.iterator();
		});
		

		JavaPairDStream<String, Integer> hashtagsCounts = hashtags.reduceByKeyAndWindow((Integer h1 ,Integer h2) -> {
			return h1 + h2;
		},Durations.seconds(30),Durations.seconds(10));
		
		
		JavaPairDStream<Integer, String> flipHashtagCounts = hashtagsCounts.transformToPair( hash -> {
			JavaPairRDD<Integer, String> flipHashtag = hash.mapToPair(f -> {
				return new Tuple2<Integer, String>(f._2(),f._1()); 
			});
			
			return flipHashtag.sortByKey(false);
		});
		JavaPairDStream<Integer, String> flipHashtagFiltered = flipHashtagCounts.filter(f ->{
			if (f._1()>100)
				return true;
			else
				return false;
		});
		
		flipHashtagFiltered.print();
		
		flipHashtagFiltered.dstream().saveAsTextFiles(outputPathPrefix, "filtered");
		
		// Start the computation
		jssc.start();              
		
		
		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);
		
		jssc.close();
		
	}
}
