package com.phylosoft.spark.learning.sql.streaming.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

trait StreamingSink {

  def start(data: DataFrame,
            trigger: Trigger = Trigger.Once(),
            outputMode: OutputMode = OutputMode.Update()): StreamingQuery

}

/*
//    import scala.concurrent.duration._
//
//    val trigger = triggerPolicy match {
//      case TRIGGER_POLICY.DEFAULT =>
//        // Default trigger (runs micro-batch as soon as it can)
//        Trigger.ProcessingTime(0L)
//      case TRIGGER_POLICY.PROCESSING_TIME =>
//        // ProcessingTime trigger with two-seconds micro-batch interval
//        Trigger.ProcessingTime(2.seconds)
//      case TRIGGER_POLICY.ONCE =>
//        // One-time trigger
//        Trigger.Once()
//      case TRIGGER_POLICY.CONTINUOUS =>
//        // Continuous trigger with one-second checkpointing interval
//        Trigger.Continuous("1 second")
//      case _ => sys.exit(1)
//    }
 */


