package com.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InMemoryDatasetExample {

	public static void main(String[] args) throws AnalysisException {
	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("SparkSqlExample")
				.master("local[*]").getOrCreate();
	
//		List<Row> inMemory = new ArrayList<Row>(); 
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
//		
//		StructField[] fields = new StructField[] {
//				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//		};
//		StructType schema=new StructType(fields);
//		Dataset<Row> dataset=spark.createDataFrame(inMemory, schema);
//	
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		//dataset.createOrReplaceTempView("logging_table");
		
	//	Dataset<Row> result=spark.sql("select level,count(datetime) from logging_table group by level order by level");
	//	Dataset<Row> result=spark.sql("select level,collect_list(datetime) from logging_table group by level order by level");
		
//		Dataset<Row> result=spark.sql("select level,date_format(datetime,'MMMM') as month,"
//				+ "count(*) as total from logging_table group by month,level order by cast(first(date_format(datetime,'M')) as int),level");
//	
	dataset = dataset.select(col("level"),date_format(col("datetime"), "MMMM").alias("month")
			,date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
		
	dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
	dataset = dataset.orderBy(col("monthnum"),col("level"));
	dataset =dataset.drop(col("monthnum"));
	dataset.show(100);
		
	//result = result.drop("monthnum");
	//	result.show(100);
	
//		result.createOrReplaceTempView("result_table");
//		Dataset<Row> result_sum=spark.sql("select sum(total) from result_table");
//		result_sum.show();
		spark.close();
	}

}
