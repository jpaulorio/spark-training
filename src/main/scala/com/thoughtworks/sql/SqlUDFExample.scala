package com.thoughtworks.sql

import org.apache.spark.sql.SparkSession
import com.thoughtworks.utils.DFUtils

object SqlUDFExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe UDF Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.udf.register("startsWith", DFUtils.startsWith(_:String,_:String):Boolean)

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    usersParquetDF.createTempView("users")

    //----------------------------------------------------------------------------------------------//

    spark.sql("select CASE WHEN startswith(Name, 'J') THEN 'Nice name' ELSE 'Whatever' END from users")
      .show(false)

    //----------------------------------------------------------------------------------------------//

    spark.sql("select * from users where startswith(Company, 'T')").show(false)


    while (true) {}
  }
}

