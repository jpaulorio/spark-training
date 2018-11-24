package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object NeatTotal {
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

    val spark = SparkSession
      .builder()
      .master("local")
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

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dfOrdersWithItems = dfOrdersRaw
      .join(dfOrderItemsRaw, "OrderId")
      .as("ooi")
      .join(dfProductsRaw.as("p"), col("ooi.ProductId") === col("p.ProductId"))

    val total = dfOrdersWithItems.agg(sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity" ).as("total"))
      .select("total").first().getAs[Double]("total")

    val locale = new java.util.Locale("pt", "BR")
    val formatter = java.text.NumberFormat.getCurrencyInstance(locale)
    val totalFormatted = formatter.format(total)

    log.info(s"O total de vendas foi $totalFormatted")
    println(s"O total de vendas foi $totalFormatted")
    //185.670.050.745
    //cento e oitenta e cinco bilhões, seiscentos e setenta milhões, cinquenta mil e setecentos e quarenta e cinco
  }
}
