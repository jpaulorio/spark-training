package core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}
import schemas.User

object StagesWithShuffleExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val users = List(User("John", "ThoughtWorks"), User("Jane", "Google"), User("Bob", "Oracle"))
    val usersDF = spark.sparkContext.parallelize(users, 1).toDF

    val mappedUsersDF = usersDF.withColumn("Initial", substring(col("Name"), 0, 1))

    val groupedUsers = mappedUsersDF.groupBy("Initial").count()

    groupedUsers.collect().foreach(println)

    while (true) {}
  }
}
