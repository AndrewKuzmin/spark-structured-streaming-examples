package com.phylosoft.spark.learning.sql.streaming.sink.memory

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
  * Created by Andrew Kuzmin on 3/16/2019.
  */
class MemorySink(trigger: Trigger = Trigger.Once(),
                 outputMode: OutputMode = OutputMode.Update())
  extends StreamingSink {

  override def writeStream(data: DataFrame): StreamingQuery = {
    data.writeStream
      .format("memory")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation + "/memory")
      .start()
  }

}