package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstream

import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import com.phylosoft.spark.learning.sql.streaming.source.rate.AdRateSources
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Inner Join with Watermarking
  */
object InnerJoinWithWatermarkingApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("InnerJoinWithWatermarkingApp") {

      override def createStreamingSources(spark: SparkSession): AdRateSources =
        new AdRateSources(spark, rowsPerSecond = "10")

      override def join(impressions: DataFrame, clicks: DataFrame): DataFrame = {

        import org.apache.spark.sql.functions._
        import spark.implicits._

        // Define watermarks
        val impressionsWithWatermark = impressions
          .select($"adId".as("impressionAdId"), $"impressionTime")
          .withWatermark("impressionTime", "10 seconds ") // max 1 minutes late

        val clicksWithWatermark = clicks
          .select($"adId".as("clickAdId"), $"clickTime")
          .withWatermark("clickTime", "20 seconds") // max 2 minutes late

        impressionsWithWatermark.join(clicksWithWatermark,
          expr(
            """
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
          )
        )

      }

      override def createStreamingSink: StreamingSink = {
        import scala.concurrent.duration._
        new ConsoleSink(trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
      }

    }

    processor.start()

  }

}
