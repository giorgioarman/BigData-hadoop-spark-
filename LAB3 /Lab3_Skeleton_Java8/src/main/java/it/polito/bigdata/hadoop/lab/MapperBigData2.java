package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
	private TopKVector<WordCountWritable> top100;
	
	protected void setup(Context context) {
		top100 = new TopKVector<WordCountWritable>(100);
		
	}	
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	top100.updateWithNewElement(new WordCountWritable(new String(key.toString()), new Integer(Integer.parseInt(value.toString()))));
    	
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the top1 date and income related to this mapper
    	Vector<WordCountWritable> top100Objects = top100.getLocalTopK();
    	
    	for (WordCountWritable value : top100Objects)
    	{
    		context.write(NullWritable.get(), new WordCountWritable(value));
    		//context.write(new Text(value.getWord()), new IntWritable(value.getCount()));
    		//context.write(new Text("Hello"), new IntWritable(2));
    	}
		
	}
}
