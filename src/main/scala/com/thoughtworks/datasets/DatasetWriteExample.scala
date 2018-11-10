package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetWriteExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    FileUtils.deleteQuietly(new File("data/output"))

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.write.parquet("data/output/parquet/")

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.write.csv("data/output/csv/")

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.write.json("data/output/json/")

    while (true) {}

  }
}


