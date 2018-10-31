package com.databricks.example

import com.thoughtworks.utils.DFUtils

/**
  * Created by bill on 5/8/17.
  */
class SqlReadExampleTest extends BaseSpec {

  import testImplicits._

  "The UDF" should "work as expected" in {
    assert(DFUtils.startsWith("something", "s") == true)
  }

  "Well formatted strings" should "just work" in {
    spark.udf.register("startswith", DFUtils.startsWith(_: String, _:String): Boolean)
    val authors = Seq("bill,databricks", "matei,databricks")
    val authorsDF = spark
      .sparkContext
      .parallelize(authors)
      .toDF("raw")
      .selectExpr("split(raw, ',') as values")
      .selectExpr("startswith(values[0],'b') as startsWithB", "values[1] as company")

    assert(authorsDF.count() == 2)
  }

  "Poorly formatted strings" should "not work" in {
    spark.udf.register("startswith", DFUtils.startsWith(_: String,_:String): Boolean)
    val authors = Seq(
      "matei",
      "bill",
      "bill,databricks,matei,databricks",
      "poorly formatted")

    val authorsDF = spark
      .sparkContext
      .parallelize(authors)
      .toDF("raw")
      .selectExpr("split(raw, ',') as values")
      .where("size(values) == 2")
      .selectExpr("pointlessUDF(values[0]) as name", "values[1] as company")

    authorsDF.show()
    assert(authorsDF.where("name is not null").count() == 0)
  }
}
