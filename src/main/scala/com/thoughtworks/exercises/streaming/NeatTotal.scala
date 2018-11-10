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
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("parseTimestampWithTimezone", DFUtils.parseTimestampWithTimezone(_: String): Long)

    FileUtils.deleteQuietly(new File("../data/output/streaming"))
    FileUtils.deleteQuietly(new File("../data/output/parquet/streaming"))

    val availableProducts = Product.getAvailableProducts()

    val orderGeneratorTask = runGeneratorOnSocket(9999,
      () => {
        val order = Order.generateRandom(Store.getAvailableStores(), availableProducts)
        val orderString = order.orderToCSVString().replace("\n", "")
        val orderItemsString = order.itemsToCSVString().mkString("|")
          .replace(';', ',').replace("\n", "")
        orderString + ";" + orderItemsString
        //029201d3-9552-48dc-9c5d-be6d943af913;214d018b-0a74-4bbd-9069-818aaa9b6aca;2018-04-02T11:30;5a661222-f64d-4cea-bc9f-25fcde71935f;029201d3-9552-48dc-9c5d-be6d943af913,8410660b-ed85-4bdb-8a90-2e58e9260245,37.532352746929305,9|029201d3-9552-48dc-9c5d-be6d943af913,ae9da70f-9634-4c84-b37d-7530fcef834b,32.44139481304001,7|029201d3-9552-48dc-9c5d-be6d943af913,b0c0b70c-0275-40e5-84c0-a9d79de56f39,74.3826793933618,10
      })

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")

    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products.csv"

    //----------------------------------------------------------------------------------------------//

    val dfProductsRaw = availableProducts.toDF("ProductId", "Name", "Category", "Price")

    val ordersStreamDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
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
    //.orderBy($"Timestamp")

    val ordersDFQuery =
      totalRevenueDF
        .writeStream
        //        .format("parquet")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .format("console")
        .option("checkpointLocation", "../data/output/streaming/checkpoint")
        .option("path", "../data/output/parquet/streaming")
        .outputMode(OutputMode.Append())
        .queryName("Total Revenue")
        .start()

    ordersDFQuery.awaitTermination()

    //    orderGeneratorTask.onComplete {
    //      case Success(_) => println("Exited with success")
    //      case Failure(e) => println(s"Exited with an exception: ${e.getMessage}")
    //    }
  }
}
