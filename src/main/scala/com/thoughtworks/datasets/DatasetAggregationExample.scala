package com.thoughtworks.datasets

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.thoughtworks.schemas.{EnhancedUser, User, UserAggregates, UserDetails}
import com.thoughtworks.utils.DFUtils


object DatasetAggregationExample extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val enhancedUsersDS = spark.read.parquet("data/parquet/users-enhanced/").as[EnhancedUser]

    //----------------------------------------------------------------------------------------------//

    val usersGroupedByKey = enhancedUsersDS.groupByKey(_.Company)

    usersGroupedByKey.keys.show(false)
    usersGroupedByKey.count().show(false)

    usersGroupedByKey.keys.show(false)
    usersGroupedByKey.mapValues(user => user).count().show(false)

    usersGroupedByKey.mapGroups((key, users) => {
      val usersList = users.toList
      (key, usersList.size, usersList.size)
    })
      .show(false)

    usersGroupedByKey.mapGroups((key, users) => {
      val usersList = users.toList
      UserAggregates(key, usersList.maxBy(_.Age).Age, usersList.maxBy(_.Salary).Salary)
    })
      .show(false)

    usersGroupedByKey.mapGroups((key, users) => {
      val usersList = users.toList
      UserAggregates(key, usersList.minBy(_.Age).Age, usersList.minBy(_.Salary).Salary)
    })
      .show(false)

    usersGroupedByKey.mapGroups((key, users) => {
      val usersList = users.toList
      UserAggregates(key,
        usersList.map(_.Age).sum / usersList.size,
        usersList.map(_.Salary).sum / usersList.size)
    })
      .show(false)

    usersGroupedByKey.agg(col("value").startsWith("T").as[Boolean]).show(false)

    usersGroupedByKey.agg(expr("substring(value,1,1) = 'T'").as[Boolean]).show(false)

    usersGroupedByKey.agg(max(col("Age")).as[Integer], min(col("Salary")).as[Double]).show(false)

    //----------------------------------------------------------------------------------------------//

    val usersGrouped = enhancedUsersDS.groupBy($"Company")

    usersGrouped.count().show(false)

    usersGrouped.max("Age", "Salary").show(false)

    usersGrouped.agg(col("Company").startsWith("T").as[Boolean]).show(false)

    usersGrouped.agg(expr("substring(Company,1,1) = 'T'").as[Boolean]).show(false)

    usersGrouped.agg(max(col("Age")).as[Integer], min(col("Salary")).as[Double]).show(false)

    while (true) {}

  }
}


