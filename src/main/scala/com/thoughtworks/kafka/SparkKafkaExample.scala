package com.thoughtworks.kafka

import java.util.Properties

import com.thoughtworks.models.Product
import com.thoughtworks.utils.DFUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SparkKafkaExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      //      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Spark Kafka Example")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("parseTimestampWithTimezone", DFUtils.parseTimestampWithTimezone(_: String): Long)

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
    val checkpointBucketKafka = s"$baseBucket/$username/$dataFilesBucket/streaming/kafka/checkpoint"
    val outputtBucket = s"$baseBucket/$username/$dataFilesBucket/streaming/output"

    //----------------------------------------------------------------------------------------------//

    val dfProductsRaw = availableProducts.toDF("ProductId", "Name", "Category", "Price")

    val topic = "orders"
    val avroTopic = "orders-avro"
    val brokers =
      "a41eb0771ed9211e8aa15029578fb917-1813474711.us-east-1.elb.amazonaws.com:9092, " +
        "a42059552ed9211e8aa15029578fb917-1584479926.us-east-1.elb.amazonaws.com:9092" +
        "a421f0772ed9211e8aa15029578fb917-1025101154.us-east-1.elb.amazonaws.com:9092"

    val ordersStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
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
      .groupBy(window($"Timestamp", "5 minutes", "30 seconds").as("TimeStamp"))
      .agg(sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity").as("Total"))
    //        .orderBy($"Timestamp")

    val ordersDFQueryKafka =
      ordersWithItemsAndProductsDF
        .selectExpr("CAST(Timestamp as STRING) as key", "CAST(Discount as STRING) as value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", topic + "-out")
        .option("checkpointLocation", checkpointBucketKafka)
        .start()

    val ordersDFQuery =
      totalRevenueDF
        .writeStream
        //.format("parquet")
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("25 seconds"))
        .option("checkpointLocation", checkpointBucket)
        //.option("path", outputtBucket)
        .outputMode(OutputMode.Append())
        .queryName("Total Revenue")
        .start()

    ordersDFQueryKafka.awaitTermination()
    ordersDFQuery.awaitTermination()
  }
}
