package com.thoughtworks.exercises.streaming

import java.io.File
import java.util.Properties

import com.thoughtworks.models.{Order, Product, Store}
import com.thoughtworks.utils.DFUtils
import com.thoughtworks.utils.SocketServer.runGeneratorOnSocket
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object NeatTotal {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      //.master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("parseTimestampWithTimezone", DFUtils.parseTimestampWithTimezone(_: String): Long)

//    FileUtils.deleteQuietly(new File("data/output/streaming"))
//    FileUtils.deleteQuietly(new File("data/output/parquet/streaming"))

    val availableProducts = Product.getAvailableProducts()

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")
    val dataGeneratorHost = properties.getProperty("data_generator_host")
    val dataGeneratorPort = properties.getProperty("data_generator_port")

    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products"
    val checkpointBucket = s"$baseBucket/$username/$dataFilesBucket/streaming/checkpoint"
    val outputtBucket = s"$baseBucket/$username/$dataFilesBucket/streaming/output"

    //----------------------------------------------------------------------------------------------//

    val dfProductsRaw = availableProducts.toDF("ProductId", "Name", "Category", "Price")

    val ordersStreamDF = spark
      .readStream
      .format("socket")
      .option("host", dataGeneratorHost)
      .option("port", dataGeneratorPort)
      .load()

    val ordersWithItemsDF = ordersStreamDF
      .withColumn("Fields", split($"value", ";"))
      .drop($"value")
      .withColumn("OrderId", $"Fields".getItem(0))
      .withColumn("CustomerId", $"Fields".getItem(1))
      .withColumn("Timestamp", $"Fields".getItem(2))
      .withColumn("StoreId", $"Fields".getItem(3))
      .withColumn("Items", split($"Fields".getItem(4), "\\|"))
      .select($"*", explode($"Items").as("Item"))
      .drop($"Fields")
      .withColumn("ItemFields", split($"Item", ","))
      .withColumn("ProductId", $"ItemFields".getItem(1))
      .withColumn("Discount", $"ItemFields".getItem(2))
      .withColumn("Quantity", $"ItemFields".getItem(3))
      .drop($"Items")
      .drop($"Item")
      .drop($"ItemFields")
      .withColumn("Timestamp", expr("CAST(parseTimestampWithTimezone(Timestamp) / 1000 AS TIMESTAMP)"))

    val ordersWithItemsAndProductsDF = ordersWithItemsDF.as("ooi")
      .join(dfProductsRaw.as("p"), col("ooi.ProductId") === col("p.ProductId"))

    val totalRevenueDF = ordersWithItemsAndProductsDF
      .withWatermark("Timestamp", "10 seconds")
      .groupBy(window($"Timestamp", "2 minutes", "5 seconds").as("TimeStamp"))
      .agg(sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity").as("Total"))
//    .orderBy($"Timestamp")

    val ordersDFQuery =
      totalRevenueDF
        .writeStream
        .format("parquet")
        //.format("console")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", checkpointBucket)
        .option("path", outputtBucket)
        .outputMode(OutputMode.Append())
        .queryName("Total Revenue")
        .start()

    ordersDFQuery.awaitTermination()
  }
}
