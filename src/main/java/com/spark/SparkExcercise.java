package com.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkExcercise {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf=new SparkConf().setAppName("FirstExcersise").setMaster("local[*]");
		JavaSparkContext sc =new JavaSparkContext(conf);
		
		
		
		
		
		
		sc.close();
		
	}

}
