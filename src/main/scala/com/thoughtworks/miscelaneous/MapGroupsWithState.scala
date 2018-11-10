package com.thoughtworks.miscelaneous

import org.apache.spark.sql.SparkSession

object MapGroupsWithState extends Serializable {

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

  case class UserState(user: String, var activity: String, var start: java.sql.Timestamp, var end: java.sql.Timestamp)

  def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }

    state
  }

  import org.apache.spark.sql.streaming.GroupState

  def updateAcrossEvents(user: String, inputs: Iterator[InputRow], oldState: GroupState[UserState]) = {
    var state: UserState = if (oldState.exists) oldState.get else UserState(user,
      "",
      new java.sql.Timestamp(6284160000000L),
      new java.sql.Timestamp(6284160L))

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }

    state
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Map Groups With State Example")
      .master("local")
      .config("spark.com.thoughtworks.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import org.apache.spark.sql.streaming.GroupStateTimeout

    val static = spark.read.json("/Users/jsilva/spark-definitive-guidata/activity-data")
    val streaming = spark
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/jsilva/spark-definitive-guidata/activity-data")

    val withEventTime = streaming
      .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    import spark.implicits._

    withEventTime
      .selectExpr("User as user", "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("update")
      .start()

    Thread.sleep(20000L)
    spark.sql("SELECT * FROM events_per_window order by user, start").show(false)
  }
}
