package com.thoughtworks.dataframes

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.utils.DFUtils

object DataFrameTransformationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Transformation Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("startsWith", DFUtils.startsWith(_:String,_:String):Boolean)

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    //https://spark.apache.org/docs/2.3.1/api/sql/index.html

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select("Name").show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select("Name", "Company").show(false)

    //----------------------------------------------------------------------------------------------//

    val nameDF = usersParquetDF.select($"Name")
    nameDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val nameDS = usersParquetDF.select($"Name".as[String], $"Company".as[String])
    nameDS.show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select(col("Name")).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select(usersParquetDF.col("Name")).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.selectExpr("upper(Name)").show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select(upper(col("Name"))).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.select(when(col("Name").startsWith("J"), "Nice name")
      .otherwise("Whatever"))
      .show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.selectExpr("CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END")
      .show(false)

    //----------------------------------------------------------------------------------------------//

    val columnsToSelect = Array(
      $"Company",
      col("Name"),
      lit("some-value").as("Literal"),
      expr("concat(Name,' - ',Company)"),
      concat($"Name", lit(" - "), $"Company"),
      usersParquetDF.col("Name").as("User_Name")
    )
    usersParquetDF.select(columnsToSelect: _*).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.filter(col("Company").startsWith("T")).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.where(col("Company").startsWith("T")).show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.where("startswith(Company, 'T')").show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.where("substring(Company, 0, 1) = 'T'").show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF
      .where("substring(Company, 0, 1) = 'T'")
      .union(usersParquetDF
        .where("substring(Company, 0, 1) = 'O'"))
      .show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF
      .where("substring(Company, 0, 1) = 'T'")
      .union(usersParquetDF
        .where("substring(Company, 0, 1) = 'T'"))
      .distinct()
      .show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF
      .where("substring(Company, 0, 1) = 'T'")
      .union(usersParquetDF
        .where("substring(Company, 0, 1) = 'T'"))
      .dropDuplicates()
      .show(false)

    while (true) {}
  }
}

