package it.polito.bigdata.spark.example;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class FrequencyComparator implements Comparator<Tuple2<Integer, String>>,Serializable{

	@Override
	public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
		
		return tuple1._1().compareTo(tuple2._1());
	}

	

}
