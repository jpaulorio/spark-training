package com.thoughtworks.rdds

import org.apache.spark.sql.SparkSession
import com.thoughtworks.schemas.User

object RddExample extends Serializable {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("RDD Example")
      .master("local")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val users = List(User("John", "ThoughtWorks"), User("Jane", "Google"), User("Bob", "Oracle"))

    val usersRDD = spark.sparkContext.parallelize(users)

    //----------------------------------------------------------------------------------------------//

    val mappedRDD = usersRDD.map(u => u.Name + " works at " + u.Company)

    mappedRDD.collect().foreach(println)

    //----------------------------------------------------------------------------------------------//

    val usersFromCsvRDD = spark.sparkContext.textFile("data/csv/users/")

    val lines = usersFromCsvRDD.map(_.split(";")).collect().toList

    lines.foreach(line => println(line.toList))

    while (true) {}
  }
}
