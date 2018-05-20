package com.phylosoft.spark.learning

/**
  * Created by Andrew on 5/20/2018.
  */
object AppConfig {

  object TRIGGER_POLICY extends Enumeration {
    type TRIGGER_POLICY = Value
    val DEFAULT, PROCESSING_TIME, ONCE, CONTINUOUS = Value
  }

  object Mode extends Enumeration {
    type Mode = Value
    val REALTIME, BATCH = Value
  }

  object Source extends Enumeration {
    type Source = Value
    val KAFKA, JSON, AVRO = Value
  }

  object Sink extends Enumeration {
    type Sink = Value
    val KAFKA, JSON, AVRO = Value
  }

}
