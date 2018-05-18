package com.phylosoft.spark.learning.sql.streaming.operations.join

import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import com.phylosoft.spark.learning.sql.streaming.sink.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.RateSource
import org.apache.spark.sql.DataFrame

abstract class Processor(appName: String)
  extends SparkSessionConfiguration
    with RateSource with ConsoleSink with Logger {

  private val settings = Map("spark.app.name" -> appName)

  val spark = getSparkSession(settings)

  def start(): Unit = {

    val impressions = loadImpressions()

    val clicks = loadClicks()

    val events = join(impressions, clicks)

    val query = getQuery(events, getTriggerPolicy)
    query.awaitTermination()

  }

  import AppConfig._

  def getTriggerPolicy: TRIGGER_POLICY.TRIGGER_POLICY = TRIGGER_POLICY.PROCESSING_TIME

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

}
