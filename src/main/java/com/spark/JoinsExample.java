package com.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class JoinsExample {

	public static void main(String[] args) {
Logger.getLogger("org.apache").setLevel(Level.WARN);
		

		SparkConf conf=new SparkConf().setAppName("firstSpark").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
	
		List<Tuple2<Integer,Integer>> visits = new ArrayList<Tuple2<Integer,Integer>>();
		List<Tuple2<Integer,String>> users = new ArrayList<Tuple2<Integer,String>>();
		visits.add(new Tuple2<Integer, Integer>(4, 18));
		visits.add(new Tuple2<Integer, Integer>(6, 8));
		visits.add(new Tuple2<Integer, Integer>(10, 9));
		
		users.add(new Tuple2<Integer, String>(1, "john"));
		users.add(new Tuple2<Integer, String>(2, "Bob"));
		users.add(new Tuple2<Integer, String>(3, "Alan"));
		users.add(new Tuple2<Integer, String>(4, "Doris"));
		users.add(new Tuple2<Integer, String>(5, "Marybelle"));
		users.add(new Tuple2<Integer, String>(6, "Raquel"));
		
		JavaPairRDD<Integer, Integer> visitRdd = sc.parallelizePairs(visits);
		JavaPairRDD<Integer, String> usersRdd = sc.parallelizePairs(users);
		
		
		//visitRdd.join(usersRdd).collect().forEach(System.out::println);
		
//		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visitRdd.rightOuterJoin(usersRdd);
//		
//		joinedRdd.foreach(it -> System.out.println("user "+it._2._2+" has visit "+it._2._1.orElse(0)));
//		
	JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visitRdd.cartesian(usersRdd);
		
		joinedRdd.foreach(it -> System.out.println(it));
		
		
sc.close();
	}

}
