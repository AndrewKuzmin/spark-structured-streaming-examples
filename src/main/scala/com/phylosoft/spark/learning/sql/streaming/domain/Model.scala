package com.phylosoft.spark.learning.sql.streaming.domain

import java.sql.Timestamp

object Model {

  /** User-defined data type representing the input events */
  case class Event(sessionId: String, timestamp: Timestamp)

  /**
    * User-defined data type for storing a session information as state in mapGroupsWithState.
    *
    * @param numEvents        total number of events received in the session
    * @param startTimestampMs timestamp of first event received in the session when it started
    * @param endTimestampMs   timestamp of last event received in the session before it expired
    */
  case class SessionInfo(numEvents: Int, startTimestampMs: Long, endTimestampMs: Long) {

    /** Duration of the session, between the first and last events */
    def durationMs: Long = endTimestampMs - startTimestampMs
  }

  /**
    * User-defined data type representing the update information returned by mapGroupsWithState.
    *
    * @param id         Id of the session
    * @param durationMs Duration the session was active, that is, from first event to its expiry
    * @param numEvents  Number of events received by the session while it was active
    * @param expired    Is the session active or expired
    */
  case class SessionUpdate(id: String, durationMs: Long, numEvents: Int, expired: Boolean)

}
