package com.phylosoft.spark.learning.sql.streaming.source.rate

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AdRateSources(val spark: SparkSession,
                    val rowsPerSecond: String = "5",
                    val numPartitions: String = "1")
  extends RateSource {

  def loadImpressions(): DataFrame = {
    readStream()
      .select(
        col("value").as("adId"),
        col("timestamp").as("impressionTime"))
  }

  def loadClicks(): DataFrame = {
    readStream()
      .where((rand() * 100).cast("integer") < 10) // 10 out of every 100 impressions result in a click
      .select((col("value") - 50).as("adId"), col("timestamp").as("clickTime")) // -50 so that a click with same id as impression is generated much later (i.e. delayed data).
      .where("adId > 0")
  }

}
