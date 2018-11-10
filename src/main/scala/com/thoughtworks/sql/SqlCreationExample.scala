package com.thoughtworks.sql

import org.apache.spark.sql.SparkSession

object SqlCreationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Read Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.read.parquet("data/parquet/users-enhanced/")
      .createTempView("enhanced_users")

    spark.sql("select * from enhanced_users")

    //----------------------------------------------------------------------------------------------//

    spark.read.parquet("data/parquet/users-enhanced/")
      .createOrReplaceTempView("enhanced_users")

    spark.sql("select * from enhanced_users").show(false)

    //----------------------------------------------------------------------------------------------//

    spark.read.parquet("data/parquet/users-enhanced/")
      .createGlobalTempView("enhanced_users_global")

    spark.sql("select * from global.enhanced_users_global").show(false)

    //----------------------------------------------------------------------------------------------//

    val newSparkSession = spark.newSession()

    newSparkSession.sql("select * from enhanced_users").show(false)

    newSparkSession.sql("select * from global.enhanced_users_global").show(false)

    //----------------------------------------------------------------------------------------------//

    newSparkSession.read.parquet("data/parquet/users-enhanced/")
      .createOrReplaceGlobalTempView("enhanced_users")

    newSparkSession.sql("select * from global.enhanced_users_global").show(false)

    while (true) {}
  }
}

