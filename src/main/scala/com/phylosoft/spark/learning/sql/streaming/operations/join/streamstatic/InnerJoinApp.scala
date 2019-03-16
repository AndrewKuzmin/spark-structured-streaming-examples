package com.phylosoft.spark.learning.sql.streaming.operations.join.streamstatic

import com.phylosoft.spark.learning.sql.streaming.operations.join.Processor
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.sql.streaming.sink.console.ConsoleSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object InnerJoinApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("InnerJoinApp") {

      import scala.concurrent.duration._

      override def join(impressions: DataFrame, clicks: DataFrame): DataFrame = {
        impressions.join(clicks, "adId")
      }

      override def initStreamingSink: StreamingSink = {
        new ConsoleSink(trigger = Trigger.ProcessingTime(2.seconds), outputMode = OutputMode.Append())
      }

    }

    processor.start()

  }

}
