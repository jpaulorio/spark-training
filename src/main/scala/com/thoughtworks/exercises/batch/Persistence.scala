package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}

object Persistence {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(this.getClass)

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")

    val ordersBucket = s"$baseBucket/$username/$dataFilesBucket/orders"
    val orderItemsBucket = s"$baseBucket/$username/$dataFilesBucket/orderItems"
    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products"

    val ordersParquetBucket = s"$baseBucket/$username/$dataFilesBucket/out/parquet/orders"
    val orderItemsParquetBucket = s"$baseBucket/$username/$dataFilesBucket/out/parquet/orderItems"
    val productsParquetBucket = s"$baseBucket/$username/$dataFilesBucket/out/parquet/products"


    val spark = SparkSession
    .builder()
    //.master("local")
    .appName("Data Engineering Capability Development - ETL Exercises")
    .getOrCreate()

    import spark.implicits._

    spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(ordersBucket)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("StoreId")
      .parquet(ordersParquetBucket)

    spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(orderItemsBucket)
      .repartition(200, $"OrderId")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("OrderId")
      .parquet(orderItemsParquetBucket)

    spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(productsBucket)
      .write
      .mode(SaveMode.Overwrite)
      .json(productsParquetBucket)
  }
}
