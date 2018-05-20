package com.phylosoft.spark.learning.sql.streaming.monitoring

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

/**
  * Created by Andrew on 5/20/2018.
  */
class KafkaMetrics(servers: String) extends StreamingQueryListener {

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", servers)
  kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](kafkaProperties)

  def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    producer.send(new ProducerRecord("streaming-metrics", event.progress.json))
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
  }

}
