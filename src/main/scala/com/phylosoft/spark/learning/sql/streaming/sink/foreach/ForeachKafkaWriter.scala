package com.phylosoft.spark.learning.sql.streaming.sink.foreach

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Andrew on 5/20/2018.
  */
class ForeachKafkaWriter(topic: String,
                         brokers: String,
                         func: Row => (String, String)) extends ForeachWriter[Row] {

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", brokers)
  kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")

  val results = new scala.collection.mutable.HashMap[String, String]

  // KafkaProducer can't be serialized, so we're creating it locally for each partition.
  var producer: KafkaProducer[String, String] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    producer = new KafkaProducer[String, String](kafkaProperties)
    true
  }

  override def process(row: Row): Unit = {
    val (key, value) = func(row)
    val message = new ProducerRecord[String, String](topic, key, value)
    println("sending windowed message: " + value)
    producer.send(message)
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }

}
