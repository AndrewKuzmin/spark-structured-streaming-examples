package com.phylosoft.spark.learning.sql.streaming.operations.join

import com.phylosoft.spark.learning.sql.streaming.sink.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.RateSource
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode

abstract class Processor(appName: String)
  extends SparkSessionConfiguration
    with RateSource
    with ConsoleSink
    with Logger {

  val settings = Map("spark.app.name" -> appName,
    "spark.sql.shuffle.partitions" -> "1"
  )

  def start(): Unit = {

    val impressions = loadImpressions()

    val clicks = loadClicks()

    val events = join(impressions, clicks)

    val query = getQuery(events, getTriggerPolicy, OutputMode.Append())
    query.awaitTermination()

  }

  import AppConfig._

  def getTriggerPolicy: TRIGGER_POLICY.TRIGGER_POLICY = TRIGGER_POLICY.PROCESSING_TIME

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

}
