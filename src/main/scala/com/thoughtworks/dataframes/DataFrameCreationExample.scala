package com.thoughtworks.dataframes

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import com.thoughtworks.schemas.User

object DataFrameCreationExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Read Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    //----------------------------------------------------------------------------------------------//

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    usersParquetDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val customSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("company", StringType, true)))

    val usersWithCustomSchemaDF = spark
      .read
      .format("csv")
      .option("header", true)
      .option("delimiter", ";")
      .schema(customSchema)
      .load("data/csv/users/")

    usersWithCustomSchemaDF.show(false)

    //----------------------------------------------------------------------------------------------//

    import spark.implicits._

    val usersDF = spark.sparkContext
      .textFile("data/csv/users/")
      .map(_.split(";"))
      .map(attributes => User(attributes(0), attributes(1).trim))
      .toDF()

    usersDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val usersRDD = spark.sparkContext.textFile("data/csv/users/")

    val schemaString = "name company"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = usersRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val usersDF2 = spark.createDataFrame(rowRDD, schema)

    usersDF2.show(false)

    //----------------------------------------------------------------------------------------------//

    val users = List(("JP", 25, 20000), ("Ana", 35, 25000))

    val ageSalaryDF = spark.sparkContext.parallelize(users).toDF("Name", "Age", "Salary")

    ageSalaryDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val emptyDF = spark.emptyDataFrame

    emptyDF.show(false)

    while (true) {}
  }
}

