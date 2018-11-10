package com.thoughtworks.core

import org.apache.spark.sql.SparkSession
import com.thoughtworks.schemas.User
import org.apache.log4j.LogManager

object JobsExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val log = LogManager.getLogger(this.getClass)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val users = List(User("John", "ThoughtWorks"), User("Jane", "Google"), User("Bob", "Oracle"))
    val usersDF = spark.sparkContext.parallelize(users).toDF

    log.info(s"Users before filter ${usersDF.count()}")

    val filteredUsersDF = usersDF.where("Company = 'Google'")

    log.info(s"Users after filter ${filteredUsersDF.count()}")

    while (true) {}
  }
}
