package com.phylosoft.spark.learning.sql.streaming.sink.delta

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
  * Created by Andrew Kuzmin on 7/13/2019.
  */
class DeltaSink(trigger: Trigger = Trigger.Once(),
                 outputMode: OutputMode = OutputMode.Append())
  extends StreamingSink {

  override def writeStream(data: DataFrame): StreamingQuery = {
    data.writeStream
      .format("delta")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation + "/tmp/delta/events")
      .start("/tmp/delta/events")
  }

}