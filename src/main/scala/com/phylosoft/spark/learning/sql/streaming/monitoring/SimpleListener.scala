package com.phylosoft.spark.learning.sql.streaming.monitoring

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class SimpleListener extends StreamingQueryListener {

  @volatile private var startTime: Long = 0L
  @volatile private var endTime: Long = 0L
  @volatile private var numRecs: Long = 0L

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("Query started: " + event.id)
    startTime = System.currentTimeMillis
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println("Query made progress: " + event.progress)
    numRecs += event.progress.numInputRows
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("Query terminated: " + event.id)
    endTime = System.currentTimeMillis
  }

}
