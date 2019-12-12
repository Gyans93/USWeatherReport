package cs236.gprak001;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import java.io.IOException;

public class DBMSProj {
    public static void main(String[] args) throws IOException {
        long time1 = System.nanoTime();
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSql")
                .master("local")
                .getOrCreate();
        Dataset<Row> weather_file = spark.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[0]);
        weather_file.createOrReplaceTempView("weather_stations");
        Dataset<Row> stations =
                spark.sql("SELECT * from weather_stations WHERE CTRY='US'");
//        stations.show();

        stations.createOrReplaceTempView("US_Stations");

        //Uploading all the Year wise data to Datasets
        Dataset<Row> year_2006 = spark.read()
                .option("delimiter", " ")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[1]);
        year_2006.createOrReplaceTempView("2006_file");
        Dataset<Row> year_2007 = spark.read()
                .option("delimiter", " ")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[2]);
        year_2007.createOrReplaceTempView("2007_file");
        Dataset<Row> year_2008 = spark.read()
                .option("delimiter", " ")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[3]);
        year_2008.createOrReplaceTempView("2008_file");
        Dataset<Row> year_2009 = spark.read()
                .option("delimiter", " ")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[4]);
        year_2009.createOrReplaceTempView("2009_file");
//        temp.show();

        //Joining Year wise table on US station id
        Dataset<Row> US_2006 = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2006_file._c3 AS YEARMODA, 2006_file._c7 AS Temperature FROM 2006_file INNER JOIN US_Stations ON 2006_file.`STN---` = US_Stations.USAF");
        Dataset<Row> US_2007 = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2007_file._c3 AS YEARMODA, 2007_file._c7 AS Temperature FROM 2007_file INNER JOIN US_Stations ON 2007_file.`STN---` = US_Stations.USAF");
        Dataset<Row> US_2008 = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2008_file._c3 AS YEARMODA, 2008_file._c7 AS Temperature FROM 2008_file INNER JOIN US_Stations ON 2008_file.`STN---` = US_Stations.USAF");
        Dataset<Row> US_2009 = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2009_file._c3 AS YEARMODA, 2009_file._c7 AS Temperature FROM 2009_file INNER JOIN US_Stations ON 2009_file.`STN---` = US_Stations.USAF");
//        US_2007.show();
//        US_2008.show();
//        US_2009.show();

        Dataset<Row> combinetables = US_2006.union(US_2007).union(US_2008).union(US_2009);

        combinetables.createOrReplaceTempView("UNIONVIEW");

        //Grouping the above table on State and Month and Ordered by STATE
        Dataset<Row> groupbymonth = spark.sql("SELECT AVG(Temperature) as AvgTemp, STATE, FLOOR((YEARMODA/100)%100) AS MONTH FROM UNIONVIEW WHERE STATE IS NOT NULL GROUP BY MONTH, STATE ORDER BY STATE");
//        groupbymonth.show();

        groupbymonth.createOrReplaceTempView("GrpByStateMonth");

        //Finding Max Avg Temperature and Min Avg Temperature for each state.
        Dataset<Row> maxByState = spark.sql("SELECT t1.AvgTemp as MaxTemp,  t1.STATE, t1.MONTH as MaxMonth FROM GrpByStateMonth as t1 JOIN (SELECT MAX(AvgTemp) as maxTemp, STATE FROM GrpByStateMonth GROUP BY STATE) t2 ON t1.STATE = t2.STATE  AND t1.AvgTemp = t2.maxTemp");
        Dataset<Row> minByState = spark.sql("SELECT t1.AvgTemp as MinTemp,  t1.STATE, t1.MONTH as MinMonth FROM GrpByStateMonth as t1 JOIN (SELECT MIN(AvgTemp) as minTemp, STATE FROM GrpByStateMonth GROUP BY STATE) t2 ON t1.STATE = t2.STATE  AND t1.AvgTemp = t2.minTemp");
//        maxByState.show();
//        minByState.show();
        maxByState.createOrReplaceTempView("MaxByState");
        minByState.createOrReplaceTempView("MinByState");

        //Joining both tables to get Max Temp and Min Temp by state in one table
        Dataset<Row> maxMinByState = spark.sql( "SELECT MaxByState.STATE, MaxByState.MaxTemp, MaxByState.MaxMonth as MaxMonth, MinByState.MinTemp, MinByState.MinMonth as MinMonth FROM MaxByState JOIN MinByState ON  MaxByState.STATE = MinByState.STATE");
//        maxMinByState.show();
        maxMinByState.createOrReplaceTempView("MaxMinByState");

        //Creating a new column o show the difference between Max and Min Temperatures and order them in ascending order
        Dataset<Row> statesByDifference = spark.sql("SELECT *, MaxTemp-MinTemp as Difference FROM MaxMinByState ORDER BY Difference ASC");
//        statesByDifference.show();

        //Naming months for each month number
        Dataset<Row> monthNames  = statesByDifference.withColumn("Highest_Month", functions.when(statesByDifference.col("MaxMonth").equalTo(1), "January")
                .when(statesByDifference.col("MaxMonth").equalTo(2), "February")
                .when(statesByDifference.col("MaxMonth").equalTo(3), "March")
                .when(statesByDifference.col("MaxMonth").equalTo(4), "April")
                .when(statesByDifference.col("MaxMonth").equalTo(5), "May")
                .when(statesByDifference.col("MaxMonth").equalTo(6), "June")
                .when(statesByDifference.col("MaxMonth").equalTo(7), "July")
                .when(statesByDifference.col("MaxMonth").equalTo(8), "August")
                .when(statesByDifference.col("MaxMonth").equalTo(9), "September")
                .when(statesByDifference.col("MaxMonth").equalTo(10), "October")
                .when(statesByDifference.col("MaxMonth").equalTo(11), "November")
                .when(statesByDifference.col("MaxMonth").equalTo(12), "December")
        ).withColumn("Lowest_Month", functions.when(statesByDifference.col("MinMonth").equalTo(1), "January")
                .when(statesByDifference.col("MinMonth").equalTo(2), "February")
                .when(statesByDifference.col("MinMonth").equalTo(3), "March")
                .when(statesByDifference.col("MinMonth").equalTo(4), "April")
                .when(statesByDifference.col("MinMonth").equalTo(5), "May")
                .when(statesByDifference.col("MinMonth").equalTo(6), "June")
                .when(statesByDifference.col("MinMonth").equalTo(7), "July")
                .when(statesByDifference.col("MinMonth").equalTo(8), "August")
                .when(statesByDifference.col("MinMonth").equalTo(9), "September")
                .when(statesByDifference.col("MinMonth").equalTo(10), "October")
                .when(statesByDifference.col("MinMonth").equalTo(11), "November")
                .when(statesByDifference.col("MinMonth").equalTo(12), "December")
        );

//        monthNames.show();
        monthNames.createOrReplaceTempView("tableByMontNames");

        //Ordering the columns of the output table
        Dataset<Row> monthNamesOrdered = spark.sql("SELECT STATE, MaxTemp AS Highest_Avg_Temp, Highest_Month, MinTemp AS Lowest_Avg_Temp, Lowest_Month, Difference FROM tableByMontNames");

        //Writing the final output in a CSV file at given path in the argument
        monthNamesOrdered.repartition(1).write().format("com.databricks.spark.csv").option("inferSchema", "false").option("header","true")
                .option("charset", "UTF-8").mode(SaveMode.Overwrite).save(args[5]);

        System.out.println("Time to execute the entire operation: " + (System.nanoTime() - time1)/1000000000 + "seconds");
    }
}

