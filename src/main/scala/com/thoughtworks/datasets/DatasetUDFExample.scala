package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetUDFExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    //----------------------------------------------------------------------------------------------//

    spark.udf.register("startsWith", DFUtils.startsWith(_: String, _: String): Boolean)

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.selectExpr("CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END")
      .show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.where("startswith(Company, 'T')").show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.filter(user => user.Company.startsWith("T")).show(false)

    while (true) {}

  }
}


