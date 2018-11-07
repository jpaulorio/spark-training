package com.thoughtworks.exercises

import java.util.Properties

import org.apache.spark.sql.SparkSession

object BatchExercises {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val dataFilesBucket = properties.getProperty("data_files_bucket")
    val ordersBucket = s"$baseBucket/$dataFilesBucket/orders.csv"
    val orderItemsBucket = s"$baseBucket/$dataFilesBucket/orderItems.csv"
    val productsBucket = s"$baseBucket/$dataFilesBucket/products.csv"

    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("Data Engineering Capability Development - ETL Exercises")
      .getOrCreate()

    val dfOrdersRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(ordersBucket)

    val dfOrderItemsRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(orderItemsBucket)

    val dfProductsRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(productsBucket)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dfOrdersWithItems = dfOrdersRaw
      .join(dfOrderItemsRaw, "OrderId")
      .as("ooi")
      .join(dfProductsRaw.as("p"), col("ooi.ProductId") === col("p.ProductId"))

    val total = dfOrdersWithItems.agg(sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity" ).as("total"))
      .select("total").first().getAs[Double]("total")

    println(s"O total de vendas foi ${total}")
  }
}
