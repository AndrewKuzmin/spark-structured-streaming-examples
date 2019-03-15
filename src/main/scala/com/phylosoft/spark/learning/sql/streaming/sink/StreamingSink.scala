package com.phylosoft.spark.learning.sql.streaming.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

trait StreamingSink {

  def writeStream(data: DataFrame,
                  trigger: Trigger = Trigger.Once(),
                  outputMode: OutputMode = OutputMode.Update()): StreamingQuery

}