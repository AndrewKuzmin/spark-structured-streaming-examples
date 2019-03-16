package com.phylosoft.spark.learning.sql.streaming.sink.console

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class ConsoleSink(trigger: Trigger = Trigger.Once(),
                  outputMode: OutputMode = OutputMode.Update())
  extends StreamingSink {

  override def writeStream(data: DataFrame): StreamingQuery = {
    data.writeStream
      .format("console")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation + "/console")
      .start()
  }

}
