package com.thoughtworks.catalog

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object CatalogAPIExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/Users/jsilva/spark-training/hive-warehouse")
      .getOrCreate()

    spark.catalog.listDatabases().collectAsList().asScala.foreach(println)

    //setupExample(spark)

    spark.catalog.listTables().collectAsList().asScala.foreach(println)

    val userCompany = spark.table("Users")
      .selectExpr("concat(Name, ' - ', Company)")

    val thoughtWorksUsers =
      spark.sql("select Name from Users where Company = 'ThoughtWorks'")

    userCompany.collectAsList().asScala.foreach(println)

    thoughtWorksUsers.collectAsList().asScala.foreach(println)

    while (true) {}
  }

  private def setupExample(spark: SparkSession) = {
    //FileUtils.deleteQuietly(new File("/Users/jsilva/spark-training/hive-warehouse/users"))
    if (spark.catalog.tableExists("Users")) {
      spark.sql("drop table Users")
    }
    spark.catalog.listTables().collectAsList().asScala.foreach(println)
    spark.catalog.createTable("Users", "data/parquet/users/")
    spark.catalog.listTables().collectAsList().asScala.foreach(println)
    //val usersDF = spark.read.option("header", true).option("delimiter", ";").csv("data/csv/users/")
    //usersDF.write.saveAsTable("Users")
  }
}
