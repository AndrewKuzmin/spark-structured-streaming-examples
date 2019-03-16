package com.phylosoft.spark.learning.sql.streaming.operations.join

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.AdRateSources
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Processor(appName: String)
  extends SparkSessionConfiguration
    with Logger {

  val settings = Map("spark.app.name" -> appName,
    "spark.sql.shuffle.partitions" -> "1"
  )

  def start(): Unit = {

    val sources = createStreamingSources(spark)
    val impressions = sources.loadImpressions()
    val clicks = sources.loadClicks()

    val events = join(impressions, clicks)

    val query = startStreamingSink(events, createStreamingSink)

    query.awaitTermination()

  }

  def createStreamingSources(spark: SparkSession): AdRateSources = new AdRateSources(spark)

  private def startStreamingSink[T <: StreamingSink](data: DataFrame, sink: T): StreamingQuery = {
    sink.writeStream(data)
  }

  def createStreamingSink: StreamingSink

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

}
