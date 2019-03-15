package com.phylosoft.spark.learning.sql.streaming.sink.console

import java.io.File

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class ConsoleSink()
  extends StreamingSink {

  override def writeStream(data: DataFrame,
                           trigger: Trigger,
                           outputMode: OutputMode): StreamingQuery = {

    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/console"

    data.writeStream
      .format("console")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation)
      .start()
  }

}
