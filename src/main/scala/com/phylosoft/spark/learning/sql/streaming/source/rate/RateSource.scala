package com.phylosoft.spark.learning.sql.streaming.source.rate

import com.phylosoft.spark.learning.SparkSessionConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, pmod, rand}

trait RateSource {

  this: SparkSessionConfiguration =>

  def loadImpressions(): DataFrame = {

    import spark.implicits._

    spark.readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))

  }

  def loadClicks(): DataFrame = {

    import spark.implicits._

    spark
      .readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .where((rand() * 100).cast("integer") < 10) // 10 out of every 100 impressions result in a click
      .select(($"value" - 50).as("adId"), $"timestamp".as("clickTime")) // -100 so that a click with same id as impression is generated much later.
      .where("adId > 0")

  }

  def loadUserActions(): DataFrame = {

    import spark.implicits._

    spark
      .readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .where((rand() * 100).cast("integer") < 30) // 30 out of every 100 user actions
      .select(pmod($"value", lit(9)).as("userId"), $"timestamp".as("actionTime"))
  }

}
