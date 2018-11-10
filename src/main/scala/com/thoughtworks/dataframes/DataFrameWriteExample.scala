package com.thoughtworks.dataframes

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.utils.DFUtils

object DataFrameWriteExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Write Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.udf.register("startsWith", DFUtils.startsWith(_:String,_:String):Boolean)

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    FileUtils.deleteQuietly(new File("data/output"))

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.write.parquet("data/output/parquet/")

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.write.csv("data/output/csv/")

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.write.json("data/output/json/")

    while (true) {}
  }
}

