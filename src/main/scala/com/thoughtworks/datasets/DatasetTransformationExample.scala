package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetTransformationExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("startsWith", DFUtils.startsWith(_: String, _: String): Boolean)

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    //----------------------------------------------------------------------------------------------//

    //https://spark.apache.org/docs/2.3.1/api/sql/index.html

    usersParquetDS.map(_.Name).show(false)

    usersParquetDS.map(user => (user.Name, user.Company)).show(false)

    val nameDF = usersParquetDS.select($"Name")
    nameDF.show(false)

    val nameDS = usersParquetDS.select($"Name".as[String], $"Company".as[String])
    nameDS.show(false)

    usersParquetDS.select(col("Name")).show(false)

    usersParquetDS.selectExpr("upper(Name)").show(false)

    usersParquetDS.map(_.Name.toUpperCase).show(false)

    usersParquetDS.map(user => if (user.Name.startsWith("J")) "Nice Name" else "Whatever")
      .show(false)

    usersParquetDS.selectExpr("CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END")
      .show(false)

    val columnsToSelect = Array(
      $"Company",
      col("Name"),
      lit("some-value"),
      expr("concat(Name,' - ',Company)"),
      concat($"Name", lit(" - "), $"Company"),
      usersParquetDS.col("Name").as("User_Name")
    )
    usersParquetDS.select(columnsToSelect: _*).show(false)

    usersParquetDS.filter(_.Company.startsWith("T")).show(false)

    usersParquetDS.where(col("Company").startsWith("T")).show(false)

    usersParquetDS.where("startswith(Company, 'T')").show(false)

    usersParquetDS.where("substring(Company, 0, 1) = 'T'").show(false)

    while (true) {}

  }
}


