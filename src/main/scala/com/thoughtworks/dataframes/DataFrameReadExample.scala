package com.thoughtworks.dataframes

import org.apache.spark.sql.SparkSession

object DataFrameReadExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Read Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    //----------------------------------------------------------------------------------------------//

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    usersParquetDF.show()

    //----------------------------------------------------------------------------------------------//

    val usersCSVDF = spark.read
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .csv("data/csv/users/")

    usersCSVDF.show()

    //----------------------------------------------------------------------------------------------//

    val usersJsonDF = spark.read.json("data/json/users/")

    usersJsonDF.show()

    //----------------------------------------------------------------------------------------------//

    val usersGenericDF = spark.read.format("csv").load("data/csv/users/")

    usersGenericDF.show()

    //----------------------------------------------------------------------------------------------//

    val usersGenericDF2 = spark.read.csv("data/csv/users/")

    usersGenericDF2.show()

    while (true) {}
  }
}

