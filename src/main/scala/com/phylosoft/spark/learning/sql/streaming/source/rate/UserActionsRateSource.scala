package com.phylosoft.spark.learning.sql.streaming.source.rate

import org.apache.spark.sql.functions.{col, lit, pmod, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Andrew Kuzmin on 3/16/2019.
  */
class UserActionsRateSource(spark: SparkSession)
  extends RateSource {

  def loadUserActions(): DataFrame = {
    readStream(spark)
      .where((rand() * 100).cast("integer") < 30) // 30 out of every 100 user actions
      .select(pmod(col("value"), lit(9)).as("userId"), col("timestamp").as("actionTime"))
  }

}