package com.phylosoft.spark.learning.sql.streaming.join.streamstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * https://docs.databricks.com/spark/latest/structured-streaming/examples.html#stream-stream-joins-scala
  */
object InnerJoinApp {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    val spark = SparkSession
      .builder()
      .appName("InnerJoinApp")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    import spark.implicits._

    val impressions = spark
      .readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))

    val clicks = spark
      .readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .where((rand() * 100).cast("integer") < 10) // 10 out of every 100 impressions result in a click
      .select(($"value" - 50).as("adId"), $"timestamp".as("clickTime")) // -100 so that a click with same id as impression is generated much later.
      .where("adId > 0")

//    impressions.show(10)
//    clicks.show(10)

//    Inner Join
    val events = impressions.join(clicks, "adId")

    import scala.concurrent.duration._
    import AppConfig._

    val mode = TRIGGER_POLICY.PROCESSING_TIME

    val query = mode match {
      case TRIGGER_POLICY.DEFAULT =>
        // Default trigger (runs micro-batch as soon as it can)
        events.writeStream
          .format("console")
          .start()
      case TRIGGER_POLICY.PROCESSING_TIME =>
        // ProcessingTime trigger with two-seconds micro-batch interval
        events.writeStream
          .format("console")
          .trigger(Trigger.ProcessingTime(2.seconds))
          .start()
      case TRIGGER_POLICY.ONCE =>
        // One-time trigger
        events.writeStream
          .format("console")
          .trigger(Trigger.Once())
          .start()
      case TRIGGER_POLICY.CONTINUOUS =>
        // Continuous trigger with one-second checkpointing interval
        events.writeStream
          .format("console")
          .trigger(Trigger.Continuous("1 second"))
          .start()
      case _ => sys.exit(1)
    }

    query.awaitTermination()

  }

}
