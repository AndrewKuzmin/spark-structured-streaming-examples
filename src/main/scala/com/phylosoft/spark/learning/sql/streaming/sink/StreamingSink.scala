package com.phylosoft.spark.learning.sql.streaming.sink

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait StreamingSink {

  val checkpointLocation: String = "file:///" + new File("checkpoint").getAbsolutePath

  // Default trigger (runs micro-batch as soon as it can)
  // Trigger.ProcessingTime(0L)
  //
  // ProcessingTime trigger with two-seconds micro-batch interval
  // Trigger.ProcessingTime(2.seconds)
  //
  // One-time trigger
  // Trigger.Once()
  //
  // Continuous trigger with one-second checkpointing interval
  // Trigger.Continuous("1 second")

  def writeStream(data: DataFrame): StreamingQuery

}