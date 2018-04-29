package com.phylosoft.spark.learning.sql.streaming.join.streamstream

import org.apache.spark.sql.DataFrame

object OuterJoinWithWatermarkingApp {

  def main(args: Array[String]): Unit = {

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

    }
    processor.start()


  }

}
