package com.phylosoft.spark.learning.sql.streaming.operations.stateful

import com.phylosoft.spark.learning.sql.streaming.domain.Model.{Event, SessionInfo, SessionUpdate}
import com.phylosoft.spark.learning.sql.streaming.monitoring.Monitoring
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.UserActionsRateSource
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, Trigger}

object MapGroupsWithStateApp
  extends App
    with SparkSessionConfiguration
    with GroupsWithStateFunction
    with Monitoring
    with Logger {

  val settings = Map("spark.app.name" -> "MapGroupsWithStateApp")

  spark.streams.addListener(simpleListener)

  val source = new UserActionsRateSource(spark)

  val userActions = source.loadUserActions()
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
  val query = startStreamingSink(sessions, initStreamingSink)

  query.awaitTermination()

  private def startStreamingSink[T <: StreamingSink](data: DataFrame, sink: T) : StreamingQuery = {
    sink.writeStream(data)
  }

  private def initStreamingSink: StreamingSink = {
    import scala.concurrent.duration._
    new ConsoleSink(trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
  }

}
