package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object TopRevenue {
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

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dfOrdersWithItems = dfOrdersRaw
      .join(dfOrderItemsRaw, "OrderId")
      .as("ooi")
      .join(dfProductsRaw.as("p"), col("ooi.ProductId") === col("p.ProductId"))

    val totals = dfOrdersWithItems
      .groupBy($"ooi.ProductId", $"p.Name")
      .agg(sum($"ooi.Quantity" ).as("totalQty"),
        sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity").as("totalRevenue"))
      .select($"Name", $"totalRevenue", $"totalQty")
      .orderBy($"totalRevenue".desc)
      .limit(10)
      .collect()
      .map(x => (x.getAs[String](0), x.getAs[Integer](1), x.getAs[Double](2)))

    val locale = new java.util.Locale("pt", "BR")
    val integerFormatter = java.text.NumberFormat.getIntegerInstance(locale)
    val currencyFormatter = java.text.NumberFormat.getCurrencyInstance(locale)
    val totalsFormatted = totals.map(x => (x._1, currencyFormatter.format(x._2), integerFormatter.format(x._3)))

    totalsFormatted.foreach(x => log.info(s"O total vendido do produto ${x._1} foi ${x._2} e a quantidade foi ${x._3}"))
    totalsFormatted.foreach(x => println(s"O total vendido do produto ${x._1} foi ${x._2} e a quantidade foi ${x._3}"))

  }
}
