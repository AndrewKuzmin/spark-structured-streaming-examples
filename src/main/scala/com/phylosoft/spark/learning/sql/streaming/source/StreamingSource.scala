package com.phylosoft.spark.learning.sql.streaming.source

import org.apache.spark.sql.{DataFrame, SparkSession}

trait StreamingSource {

  def readStream(spark: SparkSession): DataFrame

}
