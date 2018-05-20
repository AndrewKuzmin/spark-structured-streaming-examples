package com.phylosoft.spark.learning.sql.streaming.sink.foreach

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Andrew on 5/20/2018.
  */
class ForeachKafkaSink(kafkaProps: Properties) extends ForeachWriter[Row] {

  // KafkaProducer can't be serialized, so we're creating it locally for each partition.
  var producer: KafkaProducer[String, Row] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    producer = new KafkaProducer[String, Row](kafkaProps)
    true
  }

  override def process(value: Row): Unit = {
    val message = new ProducerRecord[String, Row]("topic", value.getString(0), value)
    println("sending windowed message: " + value)
    producer.send(message)
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }

}
