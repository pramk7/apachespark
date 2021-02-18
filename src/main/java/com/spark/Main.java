	package com.spark;
	
	import java.util.ArrayList;
	import java.util.List;
	
	import org.apache.log4j.Level;
	import org.apache.log4j.Logger;
	import org.apache.spark.SparkConf;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
	
	public class Main {
	
		public static void main(String[] args) {
			Logger.getLogger("org.apache").setLevel(Level.WARN);
			
	List<Integer> inputData=new ArrayList<>();
	inputData.add(21);
	inputData.add(26);
	inputData.add(29);
	inputData.add(82);
	inputData.add(25);
	inputData.add(42);
	SparkConf conf=new SparkConf().setAppName("firstSpark").setMaster("local[*]");
	JavaSparkContext sc=new JavaSparkContext(conf);
	
	JavaRDD<Integer> OriginalInteger = sc.parallelize(inputData);
	Integer result=OriginalInteger.reduce((value1,value2)->value1+value2);	
	JavaRDD<Tuple2<Integer,Double>> sqrtRdd = OriginalInteger.map(value -> new Tuple2(value, Math.sqrt(value)));

	JavaRDD<String> test = sc.textFile("/home/pram/Documents/vehicleSalesLeadAssignedId.csv");
	
	test.foreach(value -> System.out.print(value+","));
	
//	sqrtRdd.collect().forEach(System.out::println);
//	System.out.println(sqrtRdd.count());
//	JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
//	Long count = singleIntegerRdd.reduce((value1,value2)->value1+value2);
//	System.out.println(count);
//	System.out.println(result);
	sc.close();
		}
	
	}
