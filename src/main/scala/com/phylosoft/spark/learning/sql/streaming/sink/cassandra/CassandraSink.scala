package com.phylosoft.spark.learning.sql.streaming.sink.cassandra

import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.utils.Provider
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
  * Created by Andrew on 5/20/2018.
  */
class CassandraSink()
  extends StreamingSink {

  private val appConf = Provider.getConfig

  private val source: String = "org.apache.spark.sql.cassandra"

  private val options: Map[String, String] = Map(
    "table" -> appConf.getString("cassandra.table"),
    "keyspace" -> appConf.getString("cassandra.keyspace"))


  def start(data: DataFrame,
            trigger: Trigger = Trigger.Once(),
            outputMode: OutputMode = OutputMode.Update()): StreamingQuery = {
    data.writeStream
      .format(source)
      .options(options)
      .outputMode(outputMode)
      .start()
  }

}
