package com.thoughtworks.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.thoughtworks.utils.DFUtils

object SqlJoinExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Join Example")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val usersParquetDF = spark.read.parquet("data/parquet/users/")

    usersParquetDF.createTempView("users")

    val ageSalaryDF = createAgeSalaryDataFrame(spark)

    ageSalaryDF.createTempView("salary")

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF = spark.sql("select * from users u join salary s on (u.Name = s.Name)")

    enhancedUsersDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF2 = spark.sql("select * from users u left join salary s on (u.Name = s.Name)")

    enhancedUsersDF2.show(false)

    while (true) {}
  }


  private def createAgeSalaryDataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ageSalary = List(
      generateAgeAndSalaryForUser("Ana"),
      generateAgeAndSalaryForUser("Maria"),
      generateAgeAndSalaryForUser("Zé"),
      generateAgeAndSalaryForUser("Antonio"),
      generateAgeAndSalaryForUser("Jessica"),
      generateAgeAndSalaryForUser("Pedro"),
      generateAgeAndSalaryForUser("Filipe"),
      generateAgeAndSalaryForUser("Paulo")
    )
    val ageSalaryDF = spark.sparkContext.parallelize(ageSalary).toDF("Name", "Age", "Salary")
    ageSalaryDF
  }

  private def generateAgeAndSalaryForUser(name: String) = {
    val random = scala.util.Random
    val maxAge = 60
    val salaryMultiplier = 100000

    (name, random.nextInt(maxAge), random.nextDouble() * salaryMultiplier)
  }
}

