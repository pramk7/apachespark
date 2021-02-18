		package com.spark;
		
		import java.util.ArrayList;
		import java.util.List;
		
		import org.apache.log4j.Level;
		import org.apache.log4j.Logger;
		import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
		import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Iterables;

import scala.Tuple2;
		
		public class PairedRddDemo {
			
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
		.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(":")[0], 1L))
		.groupByKey()
		.foreach(tuple -> System.out.println(tuple._1+" has "+Iterables.size(tuple._2)+" instance"));
		
//		sc.parallelize(inputData)
//			.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(":")[0], 1L))
//			.reduceByKey((value1,value2) -> value1 + value2)
//			.foreach(value -> System.out.println(value._1+" has "+value._2+" instances"));
//			
//		JavaRDD<String> OriginalLogMessages = sc.parallelize(inputData);
//		JavaPairRDD<String, String> pairRdd=OriginalLogMessages.mapToPair(rawValue -> {
//			String[] colums=rawValue.split(":");
//			String level = colums[0];
//			String date=colums[1];
//			return new Tuple2<>(level, date);
//		});
//		
//		JavaPairRDD<String,Long> numberPairRdd = pairRdd.mapToPair(value -> new Tuple2<String, Long>(value._1, 1L));
//		JavaPairRDD<String, Long> sumRdd = numberPairRdd.reduceByKey((value1,value2) -> value1+value2);
//		sumRdd.foreach(value -> System.out.println(value._1+" has "+value._2+" instances"));
//		
		sc.close();
			}
		}
