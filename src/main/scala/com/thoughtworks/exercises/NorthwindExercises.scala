package com.thoughtworks.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NorthwindExercises extends Serializable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("Data Engineering Capability Development - Northwind Exercises")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/Users/jsilva/spark-training/hive-warehouse")
      .getOrCreate()

    import spark.implicits._

    val employees = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/csv/northwind/employees.csv")
      //.repartition(4)

    employees.printSchema()
    employees.show(false)

    val products = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/csv/northwind/products.csv")
      //.repartition(4)

    products.printSchema()
    products.show(false)

    val orders = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/csv/northwind/orders.csv")
      //.repartition(4)

    orders.printSchema()
    orders.show(false)

    val orderDetails = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/csv/northwind/order-details.csv")
      //.repartition(4)

    orderDetails.printSchema()
    orderDetails.show(false)

    //Most expensive product
    products.as("p")
      .join(products
        .select(max($"UnitPrice").as("MaxPrice"))
        .as("mp"),
        $"p.UnitPrice" === $"mp.MaxPrice")
      .select("p.ProductID", "p.ProductName", "p.UnitPrice")
      .show(false)

    //Least expensive product
    products.as("p")
      .join(products
        .select(min($"UnitPrice").as("MaxPrice"))
        .as("mp"),
        $"p.UnitPrice" === $"mp.MaxPrice")
      .select("p.ProductID", "p.ProductName", "p.UnitPrice")
      .show(false)

    //Total sales per Employee
    //|EmployeeName|TotalSales|

    employees.as("e")
      .join(orders.as("o")
        .join(orderDetails.as("od"), "OrderID")
        .selectExpr("o.EmployeeID", "(od.UnitPrice - od.Discount) * od.Quantity as ItemTotal")
        .as("ode"), "EmployeeID")
      //.repartition(4, $"EmployeeID")
      .groupBy("EmployeeID", "FirstName", "LastName")
      .sum("ItemTotal")
      .drop("EmployeeID")
      .withColumn("EmployeeName", concat(col("FirstName"), lit(" "), col("LastName")))
      .drop("FirstName")
      .drop("LastName")
      .withColumnRenamed("sum(ItemTotal)", "TotalSales")
      .select("EmployeeName", "TotalSales")
      .orderBy(desc("TotalSales"))
      .show(false)

    employees.createTempView("employees")
    orders.createTempView("orders")
    orderDetails.createTempView("orderDetails")

    spark.sql("select concat(e.FirstName, ' ', e.LastName) as EmployeeName, sum((od.UnitPrice - od.Discount) * od.Quantity) as TotalSales " +
      "from employees e " +
      "join orders o on (e.EmployeeID = o.EmployeeID) " +
      "join orderDetails od on (o.OrderId = od.OrderId) " +
      "group by EmployeeName " +
      "order by TotalSales desc")
      .show(false)


    employees.as("e")
      .join(orders.as("o")
        .join(orderDetails.as("od"), "OrderID")
        .selectExpr("o.EmployeeID", "(od.UnitPrice - od.Discount) * od.Quantity as ItemTotal").as("ode"), "EmployeeID")
      //.repartition(4, $"EmployeeID")
      .groupBy("EmployeeID", "FirstName", "LastName")
      .sum("ItemTotal")
      .drop("EmployeeID")
      .withColumn("EmployeeName", concat(col("FirstName"), lit(" "), col("LastName")))
      .drop("FirstName")
      .drop("LastName")
      .withColumnRenamed("sum(ItemTotal)", "TotalSales")
      .select("EmployeeName", "TotalSales")
      .orderBy(desc("TotalSales"))
      .show(false)


    //How many jobs are going to be generated?
    //How can we
    while (true) {}
  }
}
