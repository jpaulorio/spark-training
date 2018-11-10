package com.thoughtworks.datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.thoughtworks.schemas.{EnhancedUser, User}


object DatasetCreationExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    //----------------------------------------------------------------------------------------------//

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    usersParquetDS.show(false)

    //----------------------------------------------------------------------------------------------//

    val usersDF = spark.sparkContext
      .textFile("data/csv/users/")
      .map(_.split(";"))
      .map(attributes => User(attributes(0), attributes(1)))
      .toDS()

    usersDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val usersRDD = spark.sparkContext.textFile("data/csv/users/")

    val schemaString = "name company"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = usersRDD
      .map(_.split(","))
      .map(attributes => User(attributes(0), attributes(1).trim))

    val usersDS2 = spark.createDataset[User](rowRDD)

    usersDS2.show(false)

    //----------------------------------------------------------------------------------------------//

    val users  = List(EnhancedUser("JP", "TW", 25, 20000), EnhancedUser("Ana", "TW", 35, 25000))

    val ageSalaryDS = spark.sparkContext.parallelize(users).toDS()

    ageSalaryDS.show(false)

    //----------------------------------------------------------------------------------------------//

    val emptyDS = spark.emptyDataset[User]

    emptyDS.show(false)


    while (true) {}

  }
}


