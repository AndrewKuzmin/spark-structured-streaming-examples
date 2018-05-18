package com.phylosoft.spark.learning.sql.streaming.join

import com.phylosoft.spark.learning.SparkSessionConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.streaming.Trigger

abstract class Processor(appName: String) extends SparkSessionConfiguration {

  private val settings = Map("spark.app.name" -> appName)

  private[join] val spark = getSparkSession(settings)

  def start(): Unit = {

    import spark.implicits._

    val impressions = spark.readStream.format("rate")
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

    val events = join(impressions, clicks)

    import AppConfig._

    import scala.concurrent.duration._

    val query = getTriggerPolicy match {
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

  import AppConfig._

  def getTriggerPolicy: TRIGGER_POLICY.TRIGGER_POLICY = TRIGGER_POLICY.PROCESSING_TIME

  def join(impressions: DataFrame, clicks: DataFrame): DataFrame

}
