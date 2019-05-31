package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	
    	int countProduct = 0;
    	double totalScore = 0 ;
    	
    	for (DoubleWritable value : values){
    		totalScore =(double) totalScore +Double.parseDouble(value.toString());
    		countProduct++ ;
    		   		
    	}
    	
    	double averageRate =(double) totalScore / countProduct;
    	context.write(new Text(key), new DoubleWritable(averageRate));
    	
    }
}
