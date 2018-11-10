package com.thoughtworks.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import com.thoughtworks.utils.DFUtils

object StructuredStreamingWindowExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/Users/jsilva/spark-training/hive-warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("parseTimestampWithTimezone", DFUtils.parseTimestampWithTimezone(_: String): Long)

    val rows = spark.sparkContext.parallelize(Seq("2018-08-28T13:51:02.768-03:00"))

    val df = rows.toDF

    df.printSchema()

    val dfWithColumn =
      df.withColumn("timestamp", expr("parseTimestampWithTimezone(value)"))

    dfWithColumn.show(false)

    val socketStreamDF = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

    val wordCountsDF = socketStreamDF
      .withColumn("timestamp", expr("CAST(parseTimestampWithTimezone(value) / 1000 AS TIMESTAMP)"))
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "1 minutes", "30 seconds"), $"value")
      .count()

    val wordCountQuery =
      wordCountsDF
      .writeStream
      .format("parquet")
      .option("checkpointLocation", "data/output/streaming-window/checkpoint")
      .option("path", "data/output/parquet/streaming-window")
      .outputMode(OutputMode.Complete())
      .queryName("WordCount")
      .start()

    wordCountQuery.awaitTermination()
  }
}
