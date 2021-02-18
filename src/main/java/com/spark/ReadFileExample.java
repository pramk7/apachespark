package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReadFileExample {
	public static Set<String> borings = new HashSet<String>();
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		

SparkConf conf=new SparkConf().setAppName("firstSpark").setMaster("local[*]");
JavaSparkContext sc=new JavaSparkContext(conf);
JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
JavaRDD<String> boardingWordRdd = sc.textFile("src/main/resources/subtitles/boringwords.txt");
boardingWordRdd.collect().forEach(borings::add);

initialRdd
.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
.map(value -> value.replace("-->", "").replace(":", "").replace(",", " ").replace(".", "").replace("'", " "))
.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
.filter(value -> !value.matches("^[0-9]*$"))
.filter(word -> word.length()>3)
.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
.reduceByKey((value1,value2) -> value1+value2)
.mapToPair(value -> new Tuple2<Integer, String>(value._2, value._1))
.sortByKey(false)
.filter(value -> isNotBoring(value._2))
.saveAsTextFile("src/main/resources/subtitles/Output-boaringWord.txt");



sc.close();
	}

	/**
	 * Returns true if we think the word is "boring" - ie it doesn't seem to be a keyword
	 * for a training course.
	 */
	public static boolean isBoring(String word)
	{
		return borings.contains(word);
	}

	/**
	 * Convenience method for more readable client code
	 */
	public static boolean isNotBoring(String word)
	{
		return !isBoring(word);
	}

}
