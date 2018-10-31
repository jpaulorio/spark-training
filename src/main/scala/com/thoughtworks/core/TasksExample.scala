package com.thoughtworks.core

import org.apache.spark.sql.SparkSession
import com.thoughtworks.schemas.User

object TasksExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val users = List(User("John", "ThoughtWorks"), User("Jane", "Google"), User("Bob", "Oracle"))
    val usersDS = spark.sparkContext.parallelize(users, 8).toDS

    val mappedUsersDF = usersDS.map(u => s"Name: ${u.Name} - Company: ${u.Company}")

    mappedUsersDF.collect().foreach(println)

    while (true) {}
  }
}
