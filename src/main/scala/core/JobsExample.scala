package core

import org.apache.spark.sql.SparkSession
import schemas.User

object JobsExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val users = List(User("John", "ThoughtWorks"), User("Jane", "Google"), User("Bob", "Oracle"))
    val usersDF = spark.sparkContext.parallelize(users).toDF

    println(s"Users before filter ${usersDF.count()}")

    val filteresUsersDF = usersDF.where("Company = 'Google'")

    println(s"Users after filter ${filteresUsersDF.count()}")

    while (true) {}
  }
}
