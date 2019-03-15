package com.phylosoft.spark.learning.sql.streaming.source.rate

import org.apache.spark.sql.functions.{col, lit, pmod, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RateSources(spark: SparkSession)
  extends RateSource {

  def loadImpressions(): DataFrame = {
    readStream(spark)
      .select(
        col("value").as("adId"),
        col("timestamp").as("impressionTime"))
  }

  def loadClicks(): DataFrame = {
    readStream(spark)
      .where((rand() * 100).cast("integer") < 10) // 10 out of every 100 impressions result in a click
      .select((col("value") - 50).as("adId"), col("timestamp").as("clickTime")) // -100 so that a click with same id as impression is generated much later.
      .where("adId > 0")
  }

  def loadUserActions(): DataFrame = {
    readStream(spark)
      .where((rand() * 100).cast("integer") < 30) // 30 out of every 100 user actions
      .select(pmod(col("value"), lit(9)).as("userId"), col("timestamp").as("actionTime"))
  }

}
