package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstatic

import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

object InnerJoinApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("InnerJoinApp") {

      import scala.concurrent.duration._

      override def join(impressions: DataFrame, clicks: DataFrame): DataFrame = {
        impressions.join(clicks, "adId")
      }

      override def getTriggerPolicy: Trigger = Trigger.ProcessingTime(2.seconds)

    }

    processor.start()

  }


}
