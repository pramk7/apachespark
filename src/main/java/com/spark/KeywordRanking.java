package com.spark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KeywordRanking {
	public static Set<String> borings = new HashSet<String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		

		SparkConf conf=new SparkConf().setAppName("firstSpark").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

		JavaRDD<String> boardingWordRdd = sc.textFile("src/main/resources/subtitles/boringwords.txt");
		boardingWordRdd.collect().forEach(borings::add);
		
		JavaRDD<String> lettersonlyRdd = initialRdd.map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> removeBlankLineRdd = lettersonlyRdd.filter(value -> value.trim().length()>0);
		JavaRDD<String> justWords = removeBlankLineRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		JavaRDD<String> blankWordRemoved = justWords.filter(word -> word.trim().length()>0);
		JavaRDD<String> justIntrestingWords = blankWordRemoved.filter(word -> isNotBoring(word));
		JavaPairRDD<String, Long> pairRdd = justIntrestingWords.mapToPair(value -> new Tuple2<String,Long>(value, 1L));
		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((v1,v2)->v1+v2);
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2, tuple._1));
		JavaPairRDD<Long,String> sorted = switched.sortByKey(false);
		List<Tuple2<Long, String>> result = sorted.take(10);
		result.forEach(System.out::println);
Scanner scanner = new Scanner(System.in);
scanner.nextLine();
		
		sc.close();
	}
	public static boolean isNotBoring(String word)
	{
		return !isBoring(word);
	}
	public static boolean isBoring(String word)
	{
		return borings.contains(word);
	}

}
