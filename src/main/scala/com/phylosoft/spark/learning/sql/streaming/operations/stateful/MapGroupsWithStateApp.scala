package com.phylosoft.spark.learning.sql.streaming.operations.stateful

import com.phylosoft.spark.learning.sql.streaming.domain.Model.{Event, SessionInfo, SessionUpdate}
import com.phylosoft.spark.learning.sql.streaming.operations.join.AppConfig.TRIGGER_POLICY
import com.phylosoft.spark.learning.sql.streaming.sink.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.RateSource
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.streaming.GroupStateTimeout

object MapGroupsWithStateApp
  extends App
    with SparkSessionConfiguration
    with RateSource
    with ConsoleSink
    with GroupsWithStateFunction
    with Logger {

  val settings = Map("spark.app.name" -> "MapGroupsWithStateApp")
  val spark = getSparkSession(settings)

  val userActions = loadUserActions()
  userActions.printSchema()

  import spark.implicits._

  val events = userActions
    .withColumnRenamed("userId", "sessionId")
    .withColumnRenamed("actionTime", "timestamp")
    .as[Event]
  events.printSchema()

  // Sessionize the events. Track number of events, start and end timestamps of session, and
  // and report session updates.
  val timeTimeoutMode = "ProcessingTime"

  val sessionUpdates = timeTimeoutMode match {
    case "ProcessingTime" => events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
      sessionUpdate
    }
    case _ => events
      .withWatermark("timestamp", "2 seconds")
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.EventTimeTimeout) {
      sessionUpdate
    }
  }

  val sessions = sessionUpdates
    .select($"*")
    .where("expired == true")
  sessions.printSchema()

  // Start running the query that prints the session updates to the console
  val query = getQuery(sessions, TRIGGER_POLICY.PROCESSING_TIME)
  query.awaitTermination()

}
