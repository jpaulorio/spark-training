package com.thoughtworks.dataframes

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.utils.DFUtils

object DataFrameUDFExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe UDF Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    //----------------------------------------------------------------------------------------------//

    spark.udf.register("startsWith", DFUtils.startsWith(_:String,_:String):Boolean)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.selectExpr("CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END")
      .show(false)

    //----------------------------------------------------------------------------------------------//

    usersParquetDF.where("startswith(Company, 'T')").show(false)

    while (true) {}
  }
}

