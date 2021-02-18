package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatmapExample {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
List<String> inputData=new ArrayList<>();
inputData.add("WARN: Tuesday 4 September 0405");
inputData.add("ERROR: Tuesday 4 September 0408");
inputData.add("FATAL: Wednesday 5 September 1632");
inputData.add("ERROR: Friday 7 September 1854");
inputData.add("WARN: Saturday 8 September 1942");


SparkConf conf=new SparkConf().setAppName("firstSpark").setMaster("local[*]");
JavaSparkContext sc=new JavaSparkContext(conf);
	
	sc.parallelize(inputData)
	.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
	.filter(word -> word.length()>1).filter(value -> !value.matches("^[0-9]*$"))
	.collect().forEach(System.out::println);
	

sc.close();
	}
}
