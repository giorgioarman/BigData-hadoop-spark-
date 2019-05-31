package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	int countProduct = 0;
    	double totalScore = 0 ;
    	
    	HashMap<String, Double> productRate = new HashMap<>();
    	
    	for (Text value : values){
    		String[] ProRate = value.toString().split("_");
    		totalScore =(double) totalScore +Integer.parseInt( ProRate[1]);
    		countProduct++ ;    		
    		productRate.put(ProRate[0],Double.parseDouble( ProRate[1]));
    		   		
    	}
    	
    	double averageRate =(double) totalScore / countProduct;
    	
    	for (Map.Entry<String, Double> entry : productRate.entrySet()) {
    	    String ProductID = entry.getKey();
    	    Double ProductScore = entry.getValue();
    	    double updatedScore =  ProductScore - averageRate;
    	    
    	    context.write(new Text(ProductID), new DoubleWritable(updatedScore));
    	    
    	}
    	

		/* Implement the reduce method */
    	
    }
}
