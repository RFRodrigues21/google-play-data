package com.rafael

import org.apache.spark.sql.functions.{avg, col, desc, first, udf, when}
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
 * @author Rafael Rodrigues
 */


object App {

  def readCsvIntoDataFrame(filePath: String, spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }

  def ex1_calculateAverageSentiment(df: DataFrame): DataFrame = {
    val result = df.groupBy("App")
      .agg(
        avg(when(col("Sentiment_Polarity").isNotNull, col("Sentiment_Polarity")).otherwise(0.0))
          .alias("Average_Sentiment_Polarity")
      )
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))

    result
  }

  def writeDataFrameToCsv(df: DataFrame, outputPath: String, delimiter: String = ","): Unit = {
    df.coalesce(1) // Ensure a single output file
      .write
      .option("delimiter", delimiter)
      .mode("overwrite")
      .csv(outputPath)
  }

  def ex2_filterAndSaveBestApps(df: DataFrame, outputPath: String): Unit = {
    val filteredApps = df.filter(col("Rating").geq(4.0))
      .orderBy(desc("Rating"))

    // df.coalesce(1).write.csv(outputPath);
  }

  def convertSizeToDouble(size: Column): Column = {
    val sizeInNumber = regexp_extract(size, "\\d+", 0).cast(DoubleType)
    val sizeUnit = regexp_extract(size, "[a-zA-Z]+", 0)

    when(sizeUnit.equalTo("M"), sizeInNumber)
      .otherwise(lit(null).cast(DoubleType))
  }


  def convertToEur(price: Column): Column = {
    val priceInNumber = regexp_replace(price, "\\$", "").cast(DoubleType)
    when(priceInNumber.isNotNull, priceInNumber.*(0.9))
      .otherwise(price)
  }

  def mapStringToArray(string: Column, delimiter: String): Column = {
    split(string, delimiter)
  }

  def ex3_cleanAppDataFrame(appsDataFrame: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    appsDataFrame
      .withColumn("Size", convertSizeToDouble($"Size"))
      .withColumn("Price", convertToEur($"Price"))
      .withColumn("Genres", mapStringToArray($"Genres", ";"))
      .groupBy($"App")
      .agg(
        collect_set($"Category").as("Categories"),
        first($"Rating").as("Rating"),
        first($"Reviews").as("Reviews"),
        first($"Size").as("Size"),
        first($"Installs").as("Installs"),
        first($"Type").as("Type"),
        first($"Price").as("Price"),
        first($"Content Rating").as("Content_Rating"),
        first($"Genres").as("Genres"),
        first($"Last Updated").as("Last_Updated"),
        first($"Current Ver").as("Current_Version"),
        first($"Android Ver").as("Minimum_Android_Version")
      )
  }

  def ex4_joinDataFrames(dt_1: DataFrame, dt_3: DataFrame, spark: SparkSession, outputPath:String) = {

    val dt_1_renamed = dt_1.withColumnRenamed("App", "App_dt1")


    val joinedDataFrame = dt_3
      .join(dt_1_renamed, dt_3("App") === dt_1_renamed("App_dt1"))

    joinedDataFrame

    /*joinedDataFrame.write
      .mode(SaveMode.Overwrite) // You can change the SaveMode based on your requirement
      .option("compression", "gzip")
      .parquet(outputPath)*/
  }

  def ex5_createAndSaveMetricsDataFrame(inputDF: DataFrame, spark: SparkSession, outputPath:String): DataFrame = {
    import spark.implicits._

    // Perform aggregations
    val metricsDF = inputDF
      .groupBy($"Genres")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    metricsDF

    /*
    metricsDF.write.mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .parquet(outputPath)

     */

  }



  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName("CreateDataframe")
      .master("local")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
      .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .getOrCreate()

    var googleplaystore = readCsvIntoDataFrame("google-play-store-apps/googleplaystore.csv", spark)
    var googleplaystore_user_reviews = readCsvIntoDataFrame("google-play-store-apps/googleplaystore_user_reviews.csv", spark)

    //googleplaystore_user_reviews.show()

    //df_3.where(col("Reviews") === null ).show()


    //Ex 1
    val df_1 = ex1_calculateAverageSentiment(googleplaystore_user_reviews)
    //df_1.show()

    //Ex 2
    //ex2_filterAndSaveBestApps(googleplaystore, "./exports/ex2.csv")

    //Ex 3
    val df_3 = ex3_cleanAppDataFrame(googleplaystore, spark)
    //df_3.show()

    //Ex 4
    val ex4 = ex4_joinDataFrames(df_1,df_3,spark,"./exports/ex4.csv")
    //ex4.show()

    //Ex 5
    val df_4 = ex5_createAndSaveMetricsDataFrame(ex4,spark,"./exports/ex5.csv")
    //df_4.show()

  }


}
