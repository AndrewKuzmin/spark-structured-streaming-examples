package com.phylosoft.spark.learning.sql.streaming.source.rate

import org.apache.spark.sql.functions.{col, lit, pmod, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Andrew Kuzmin on 3/16/2019.
  */
class UserActionsRateSource(val spark: SparkSession,
                            val rowsPerSecond: String = "5",
                            val numPartitions: String = "1")
  extends RateSource {

  def loadUserActions(): DataFrame = {
    readStream()
      .where((rand() * 100).cast("integer") < 30) // 30 out of every 100 user actions
      .select(pmod(col("value"), lit(9)).as("userId"), col("timestamp").as("actionTime"))
  }

}