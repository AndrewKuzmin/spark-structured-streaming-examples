package com.phylosoft.spark.learning.sql.streaming.operations.join

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.AdRateSources
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

abstract class Processor(appName: String)
  extends SparkSessionConfiguration
    with Logger {

  val settings = Map("spark.app.name" -> appName,
    "spark.sql.shuffle.partitions" -> "1"
  )

  def start(): Unit = {

    val sources = new AdRateSources(spark)

    val impressions = sources.loadImpressions()

    val clicks = sources.loadClicks()

    val events = join(impressions, clicks)

    val query = startStreamingSink(events, new ConsoleSink())

    query.awaitTermination()

  }

  private def startStreamingSink[T <: StreamingSink](data: DataFrame, sink: T): StreamingQuery = {
    sink.writeStream(data = data, trigger = getTriggerPolicy, outputMode = OutputMode.Append())
  }

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

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
  def getTriggerPolicy: Trigger = Trigger.Once()

}
