package com.thoughtworks.sql

import org.apache.spark.sql.SparkSession
import com.thoughtworks.utils.DFUtils

object SqlAggregationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Aggregation Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.udf.register("startsWith", DFUtils.startsWith(_: String, _: String): Boolean)

    //----------------------------------------------------------------------------------------------//

    spark.read.parquet("data/parquet/users-enhanced/")
      .createTempView("enhanced_users")

    //----------------------------------------------------------------------------------------------//


    val allUsers = spark.sql("select * from enhanced_users")

    val userCount = spark.sql("select Company, count(*) as Count from enhanced_users group by Company")
    userCount.show(false)

    val maxAgeSalary = spark.sql("select Company, max(Age) as Age, max(Salary) as Salary from enhanced_users group by Company")
    maxAgeSalary.show(false)

    val minAgeSalary = spark.sql("select Company, min(Age) as Age, min(Salary) as Salary from enhanced_users group by Company")
    minAgeSalary.show(false)

    val meanAgeSalary = spark.sql("select Company, mean(Age) as Age, mean(Salary) as Salary from enhanced_users group by Company")
    meanAgeSalary.show(false)

    val avgAgeSalary = spark.sql("select Company, avg(Age) as Age, avg(Salary) as Salary from enhanced_users group by Company")
    avgAgeSalary.show(false)

    val tCompanies = spark.sql("select Company from enhanced_users group by startsWith(Company, 'T')")
    tCompanies.show(false)

    val tCompanies2 = spark.sql("select Company from enhanced_users group by substring(Company,1,1) = 'T'")
    tCompanies2.show(false)

    val maxAgeAvgSalary = spark.sql("select Company, max(Age) as Age, avg(Salary) as Salary from enhanced_users group by Company")

    while (true) {}
  }
}

