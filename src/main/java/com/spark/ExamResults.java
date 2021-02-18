package com.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExamResults {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("SparkSqlExample")
				.master("local[*]").getOrCreate();
	
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
	
		dataset = dataset.groupBy("subject").max("score");
		
		dataset.show();
		
		
		
		
		spark.close();

	}

}