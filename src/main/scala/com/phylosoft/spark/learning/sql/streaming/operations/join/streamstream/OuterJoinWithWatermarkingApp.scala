package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstream

import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object OuterJoinWithWatermarkingApp {

  def main(args: Array[String]): Unit = {

    import scala.concurrent.duration._

    val processor = new Processor("OuterJoinWithWatermarkingApp") {

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

        impressionsWithWatermark.join(
          clicksWithWatermark,
          expr(
            """
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
          ),
          "leftOuter"
        )

      }

      override def initStreamingSink: StreamingSink = {
        new ConsoleSink(trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
      }

    }

    processor.start()


  }

}
