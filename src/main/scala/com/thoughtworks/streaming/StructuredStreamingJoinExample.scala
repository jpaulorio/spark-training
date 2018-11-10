package com.thoughtworks.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.joda.time.DateTime
import com.thoughtworks.utils.DFUtils
import com.thoughtworks.utils.SocketServer.runGeneratorOnSocket

import scala.util.{Failure, Random, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object StructuredStreamingJoinExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/Users/jsilva/spark-training/hive-warehouse")
      .getOrCreate()

    import spark.implicits._

    val taskA = runGeneratorOnSocket(9998, generateRandomProduct("Loja A"))
    val taskB = runGeneratorOnSocket(9999, generateRandomProduct("Loja B"))

    val storeASales = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()
      .withColumn("_temp", split($"value", ","))
      .select($"_temp".getItem(0).as("Timestamp"),
        $"_temp".getItem(1).as("Store"),
        $"_temp".getItem(2).as("ProductId"))
      .drop("_temp")


    val storeBSales = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .withColumn("_temp", split($"value", ","))
      .select($"_temp".getItem(0).as("Timestamp"),
        $"_temp".getItem(1).as("Store"),
        $"_temp".getItem(2).as("ProductId"))
      .drop("_temp")

    val totalSales = storeASales
      .join(storeBSales, "ProductId")
      .writeStream
//      .format("parquet")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .format("console")
      .option("checkpointLocation", "data/output/streaming/stores/checkpoint")
      .option("path", "data/output/parquet/streaming/stores")
      .outputMode(OutputMode.Append())
      .queryName("TotalSales")
      .start()

    totalSales.awaitTermination()

    taskA.onComplete {
      case Success(_) => println("Exited with success")
      case Failure(e) => println(s"Exited with an exception: ${e.getMessage}")
    }

    taskB.onComplete {
      case Success(_) => println("Exited with success")
      case Failure(e) => println(s"Exited with an exception: ${e.getMessage}")
    }
  }

  def generateRandomProduct(storeName: String): () => String = {
    val productList = Map(0 -> "Book", 1 -> "DVD", 2 -> "Flowers", 3 -> "Perfume", 4 -> "Toys",
      5 -> "TV", 6 -> "Video Game", 7 -> "Macbook", 8 -> "iPhone", 9 -> "Bike")

    () => s"${DateTime.now()},$storeName,${productList.get(Random.nextInt(10))}"
  }
}
