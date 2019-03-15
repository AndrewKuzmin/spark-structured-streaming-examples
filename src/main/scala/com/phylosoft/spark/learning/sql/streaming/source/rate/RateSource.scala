package com.phylosoft.spark.learning.sql.streaming.source.rate

import com.phylosoft.spark.learning.sql.streaming.source.StreamingSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

trait RateSource extends StreamingSource {

  override def readStream(spark: SparkSession): DataFrame = {

    spark.readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .select(col("*"))

  }

}
