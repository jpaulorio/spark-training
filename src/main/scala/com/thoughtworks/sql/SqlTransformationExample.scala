package com.thoughtworks.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.utils.DFUtils

object SqlTransformationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Transformation Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.udf.register("startsWith", DFUtils.startsWith(_:String,_:String):Boolean)

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    usersParquetDF.createTempView("users")

    //https://spark.apache.org/docs/2.3.1/api/sql/index.html

    //----------------------------------------------------------------------------------------------//

    spark.sql("select Name from users").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select Name, Company from users").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select upper(Name) as Name from users").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END from users")
      .show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select * from users where startswith(Company, 'T')").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select * from users where substring(Company, 0, 1) = 'T'").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select * from users where substring(Company, 0, 1) = 'T' " +
      "union " +
      "select * from users where substring(Company, 0, 1) = 'O'")
      .show(false)

    while (true) {}
  }
}

