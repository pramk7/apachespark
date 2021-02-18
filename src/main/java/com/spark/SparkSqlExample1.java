package com.spark;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSqlExample1 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark Sql");
		//JavaSparkContext sc=new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder().appName("SparkSqlExample")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset.createOrReplaceTempView("my_student_view");

		//Dataset<Row> result = spark.sql("select * from my_student_view where subject='French'");
	//	Dataset<Row> result = spark.sql("select score,year from my_student_view where subject='French'");
		//Dataset<Row> result = spark.sql("select max(score) from my_student_view where subject='French'");
		Dataset<Row> result = spark.sql("select distinct(year) from my_student_view order by year asc");
		result.show();
		
		
		
//		dataset.show();
//		long numberOfRows = dataset.count();
//		System.out.println("there is number of rows:"+numberOfRows);
//		Row row = dataset.first();
//		String subject= row.getAs("subject").toString();
//		System.out.println(subject);
//		int year = Integer.parseInt(row.getAs("year"));
//		System.out.println(year);
//		
	//	Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art' AND year > 2007");
		//Dataset<Row> modernArt = dataset.filter(testrow -> testrow.getAs("subject").toString().equals("Modern Art")
			//	&&Integer.parseInt(testrow.getAs("year"))>=2007);
	
		
	//	Dataset<Row> modernArt = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		//modernArt.show();
		
//		Scanner scanner = new Scanner(System.in);
//		scanner.nextLine();
//		
			spark.close();
	}

}
