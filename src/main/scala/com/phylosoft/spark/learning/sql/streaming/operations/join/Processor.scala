package com.phylosoft.spark.learning.sql.streaming.operations.join

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.RateSources
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

    val sources = new RateSources(spark)

    val impressions = sources.loadImpressions()

    val clicks = sources.loadClicks()

    val events = join(impressions, clicks)

    val query = startStreamingSink(events, new ConsoleSink())
    query.awaitTermination()

  }

  private def startStreamingSink[T <: StreamingSink](data: DataFrame, sink: T): StreamingQuery = {
    import scala.concurrent.duration._
    sink.writeStream(data = data, trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
  }

  import com.phylosoft.spark.learning.AppConfig._

  def getTriggerPolicy: TRIGGER_POLICY.TRIGGER_POLICY = TRIGGER_POLICY.PROCESSING_TIME

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

}
