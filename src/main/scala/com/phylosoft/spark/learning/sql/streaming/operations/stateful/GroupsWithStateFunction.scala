package com.phylosoft.spark.learning.sql.streaming.operations.stateful

import com.phylosoft.spark.learning.sql.streaming.domain.Model.{Event, SessionInfo, SessionUpdate}
import org.apache.spark.sql.streaming.GroupState

trait GroupsWithStateFunction {

  private[stateful] val sessionUpdate = (sessionId: String,
                                         events: Iterator[Event],
                                         state: GroupState[SessionInfo]) => {
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
      state.setTimeoutDuration("5 seconds")
      SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
    }
  }

}
