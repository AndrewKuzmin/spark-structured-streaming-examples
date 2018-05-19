package com.phylosoft.spark.learning.sql.streaming.operations.stateful

import com.phylosoft.spark.learning.sql.streaming.domain.Model.{Event, SessionInfo, SessionUpdate}
import com.phylosoft.spark.learning.sql.streaming.operations.join.AppConfig.TRIGGER_POLICY
import com.phylosoft.spark.learning.sql.streaming.sink.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.RateSource
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object MapGroupsWithStateApp
  extends App
    with SparkSessionConfiguration with RateSource with ConsoleSink with Logger {

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
  val sessionUpdates = events
    .groupByKey(event => event.sessionId)
    .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

    case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

      // If timed out, then remove session and send final update
      if (state.hasTimedOut) {
        val finalUpdate =
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
        state.remove()
        finalUpdate
      } else {
        // Update start and end timestamps in session
        val timestamps = events.map(_.timestamp.getTime).toSeq
        val updatedSession = if (state.exists) {
          val oldSession = state.get
          SessionInfo(
            oldSession.numEvents + timestamps.size,
            oldSession.startTimestampMs,
            math.max(oldSession.endTimestampMs, timestamps.max))
        } else {
          SessionInfo(timestamps.size, timestamps.min, timestamps.max)
        }
        state.update(updatedSession)

        // Set timeout such that the session will be expired if no data received for 10 seconds
        state.setTimeoutDuration("10 seconds")
        SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
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
