package com.phylosoft.spark.learning.sql.streaming.sink.console

import java.io.File

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class ConsoleSink(trigger: Trigger = Trigger.Once(), outputMode: OutputMode = OutputMode.Update())
  extends StreamingSink {

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

  override def writeStream(data: DataFrame): StreamingQuery = {

    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/console"

    data.writeStream
      .format("console")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation)
      .start()
  }

}
