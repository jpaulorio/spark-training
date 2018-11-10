package com.thoughtworks.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameAggregationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Aggregation Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val enhancedUsersDF = spark.read.parquet("data/parquet/users-enhanced/")

    //----------------------------------------------------------------------------------------------//

    val usersGrouped = enhancedUsersDF.groupBy("Company")

    usersGrouped.count().show(false)

    usersGrouped.max("Age", "Salary").show(false)

    usersGrouped.min("Age", "Salary").show(false)

    usersGrouped.mean("Age", "Salary").show(false)

    usersGrouped.avg("Age", "Salary").show(false)

    usersGrouped.agg(col("Company").startsWith("T")).show(false)

    usersGrouped.agg(expr("substring(Company,1,1) = 'T'")).show(false)

    usersGrouped.agg(Map("Age" -> "max", "Salary" -> "avg")).show(false)

    while (true) {}
  }
}

