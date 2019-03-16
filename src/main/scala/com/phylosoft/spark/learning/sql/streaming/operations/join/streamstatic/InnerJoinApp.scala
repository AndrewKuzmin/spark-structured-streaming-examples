package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstatic

import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.AdRateSources
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object InnerJoinApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("InnerJoinApp") {

      override def createStreamingSources(spark: SparkSession): AdRateSources =
        new AdRateSources(spark, rowsPerSecond = "10")

      override def join(impressions: DataFrame, clicks: DataFrame): DataFrame = {
        impressions.join(clicks, "adId")
      }

      override def createStreamingSink: StreamingSink = {
        import scala.concurrent.duration._
        new ConsoleSink(trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
      }

    }

    processor.start()

  }

}
