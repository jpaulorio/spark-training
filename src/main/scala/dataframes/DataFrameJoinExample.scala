package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameJoinExample extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - Dataframe Join Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    val usersParquetDF = spark.read.parquet("../data/parquet/users/")

    val ageSalaryDF = createAgeSalaryDataFrame(spark)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF = usersParquetDF.join(ageSalaryDF, "Name")

    enhancedUsersDF.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF2 = usersParquetDF.join(ageSalaryDF, Seq("Name"), "left")

    enhancedUsersDF2.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF3 = usersParquetDF.join(ageSalaryDF,
        usersParquetDF.col("Name") === ageSalaryDF.col("Name"))

    enhancedUsersDF3.show(false)

    //----------------------------------------------------------------------------------------------//

    val enhancedUsersDF4 = usersParquetDF.as("l").join(ageSalaryDF.as("r"),
    $"l.Name" === $"r.Name")

    enhancedUsersDF4.show(false)

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

