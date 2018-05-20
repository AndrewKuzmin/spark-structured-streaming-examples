package com.phylosoft.spark.learning.sql.streaming.sink.foreach

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Andrew on 5/20/2018.
  */
class KafkaRowSink(topic: String,
                   brokers: String,
                   func: Row => (String, String))
  extends StreamingSink {

  override def start(data: DataFrame,
                     trigger: Trigger,
                     outputMode: OutputMode): StreamingQuery = {
    val writer = new ForeachKafkaWriter(topic, brokers, func)
    data
      .writeStream
      .foreach(writer)
      .outputMode(outputMode)
      .trigger(trigger)
      .start()
  }

}
