package com.phylosoft.spark.learning.sql.streaming.sink.kafka

import java.io.File
import java.util.Properties

import com.phylosoft.spark.learning.Params
import com.phylosoft.spark.learning.sql.streaming.sink.StreamingSink
import com.phylosoft.spark.learning.utils.Provider
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Andrew on 5/20/2018.
  */
class KafkaSink(private val spark: SparkSession,
                private val params: Params,
                private val kafkaProps: Properties)
  extends StreamingSink {

  def start(data: DataFrame,
            trigger: Trigger = Trigger.Once(),
            outputMode: OutputMode = OutputMode.Update()): StreamingQuery = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dataTopicPrefix = Provider.getConfig.getString("kafka.topics.data_stream_prefix")

    //      Provider.getConfig.getString("spark.checkpoint_location_importer")
    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/" + dataTopicPrefix

    val query = data
      .select($"*", concat_ws("-", lit(dataTopicPrefix), $"columnA").alias("topic"))
      .selectExpr("topic",
        "CAST(columnA AS STRING) AS key",
        "to_json(struct(columnA, columnB, columnC, columnD, columnE, timestamp)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Provider.getConfig.getString("kafka.bootstrap_servers"))
      //        .option("topic", topic)
      //        .trigger(Trigger.ProcessingTime(3.seconds))
      //        .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointLocation)
      .start()

    query

  }

}