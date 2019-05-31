package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] items = value.toString().split(",");

		for (int pro1 = 1; pro1 < items.length; pro1++) {
			for (int pro2 = 1; pro2 < items.length; pro2++) {
				if (pro1 != pro2) {
					if (items[pro1].compareTo(items[pro2]) < 0)
						context.write(new Text(items[pro1] + "," + items[pro2]), new IntWritable(1));
					else
						context.write(new Text(items[pro2] + "," + items[pro1]), new IntWritable(1));

				}
			}
		}

		/* Implement the map method */
	}
}
