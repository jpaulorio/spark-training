package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetReadExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    //----------------------------------------------------------------------------------------------//

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    usersParquetDS.show()

    //----------------------------------------------------------------------------------------------//

    val usersCSVDS = spark.read.option("header", true).option("infer_schema", true).option("delimiter", ";")
      .csv("data/csv/users/").as[User]

    usersCSVDS.show()

    //----------------------------------------------------------------------------------------------//

    val usersJsonDS = spark.read.json("data/json/users/").as[User]

    usersJsonDS.show()

    //----------------------------------------------------------------------------------------------//

    val usersGenericDS = spark.read.format("csv")
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .load("data/csv/users/").as[User]

    usersGenericDS.show()

    while (true) {}

  }
}


