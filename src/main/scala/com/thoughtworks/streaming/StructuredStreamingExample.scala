package com.thoughtworks.streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import com.thoughtworks.utils.{DFUtils, SocketServer}

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import SocketServer._
import org.joda.time.DateTime

object StructuredStreamingExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/Users/jsilva/spark-training/hive-warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("reverse", DFUtils.reverse(_: String): String)

    FileUtils.deleteQuietly(new File("data/output/streaming"))
    FileUtils.deleteQuietly(new File("data/output/parquet/streaming"))

    val task = runGeneratorOnSocket(9999, () => DateTime.now)

    //----------------------------------------------------------------------------------------------//

    val socketStreamDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val wordReverseDF = socketStreamDF
      .withColumn("reverse", expr("reverse(value)"))
      .withColumn("minutes", substring($"value", 0, 16))

    val wordReverseQuery =
      wordReverseDF
        .writeStream
        //.partitionBy("minutes")
        .format("parquet")
        .trigger(Trigger.ProcessingTime("20 seconds"))
        //.format("console")
        .option("checkpointLocation", "data/output/streaming/checkpoint")
        .option("path", "data/output/parquet/streaming")
        .outputMode(OutputMode.Append())
        .queryName("WordReverse")
        .start()

    wordReverseQuery.awaitTermination()

    task.onComplete {
      case Success(_) => println("Exited with success")
      case Failure(e) => println(s"Exited with an exception: ${e.getMessage}")
    }

  }
}
