package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstream

import com.phylosoft.spark.learning.sql.streaming.operations.join.AppConfig.TRIGGER_POLICY
import com.phylosoft.spark.learning.sql.streaming.operations.join.AppConfig.TRIGGER_POLICY.TRIGGER_POLICY
import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import org.apache.spark.sql.DataFrame

/**
  *
  */
object InnerJoinApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("InnerJoinApp") {

      override def join(impressions: DataFrame, clicks: DataFrame): DataFrame = {
        impressions.join(clicks, "adId")
      }

      override def getTriggerPolicy: TRIGGER_POLICY = TRIGGER_POLICY.PROCESSING_TIME

    }

    processor.start()

  }

}
