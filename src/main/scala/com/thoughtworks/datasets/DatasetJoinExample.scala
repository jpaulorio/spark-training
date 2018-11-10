package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetJoinExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("startsWith", DFUtils.startsWith(_: String, _: String): Boolean)

    val usersParquetDS = spark.read.parquet("data/parquet/users/").as[User]

    val userDetails = createUserDetailsDataset(spark)

    //----------------------------------------------------------------------------------------------//

    usersParquetDS.join(userDetails, "Name").printSchema()

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDS = usersParquetDS.join(userDetails, "Name").as[EnhancedUser]

    enhancedUsersDS.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF2 = usersParquetDS.join(userDetails, Seq("Name"), "left")

    enhancedUsersDF2.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF3 = usersParquetDS.join(userDetails,
        usersParquetDS.col("Name") === userDetails.col("Name"))

    enhancedUsersDF3.show(false)

    while (true) {}

  }

  private def createUserDetailsDataset(spark: SparkSession): Dataset[UserDetails] = {
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

    val ageSalaryDS = spark.sparkContext.parallelize(ageSalary).toDS
    ageSalaryDS
  }

  private def generateAgeAndSalaryForUser(name: String): UserDetails = {
    val random = scala.util.Random
    val maxAge = 60
    val salaryMultiplier = 100000

    UserDetails(name, random.nextInt(maxAge), random.nextDouble() * salaryMultiplier)
  }
}


