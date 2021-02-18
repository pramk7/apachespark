package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
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

public class pivotTableExample {

	public static void main(String[] args) throws AnalysisException {
	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("SparkSqlExample")
				.master("local[*]").getOrCreate();
	
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
	
		
		dataset = dataset.select(col("level"),date_format(col("datetime"), "MMMM").alias("month")
			,date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
		
		Object[] month = new Object[]{"January","February","March","April","May","June","July","August","September","October","November","December"};
		List<Object> columns = Arrays.asList(month);
	
		dataset = dataset.groupBy(col("level")).pivot("month",columns).count().na().fill(0);
		dataset.show(100);
		
	
	spark.close();
	}

}
