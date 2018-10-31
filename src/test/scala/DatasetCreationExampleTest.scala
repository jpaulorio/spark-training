package com.databricks.example

import com.thoughtworks.schemas.User
import com.thoughtworks.utils.DSUtils

/**
  * Created by bill on 3/30/17.
  */


class DatasetCreationExampleTest extends BaseSpec {
  import testImplicits._

  "Well formatted strings" should "just work" in {
    val authors = Seq("bill,databricks", "matei,databricks")
    val authorsRDD = spark
      .sparkContext
      .parallelize(authors)
      .map(DSUtils.createPersonFromString(_))

    val authorsDataset = spark.createDataFrame(authorsRDD).as[User]

    assert(authorsDataset.count() == 2)
  }

  "Poorly formatted strings" should "not work" in {
    val authors = Seq(
      "matei",
      "bill",
      "bill,databricks,matei,databricks",
      "poorly formatted")

    val authorsDataset = spark
      .sparkContext
      .parallelize(authors)
      .map(DSUtils.createPersonFromString(_))
      .toDF()
      .as[User]

    authorsDataset.show()
    assert(authorsDataset.where("name is not null").count() == 0)
  }

}
