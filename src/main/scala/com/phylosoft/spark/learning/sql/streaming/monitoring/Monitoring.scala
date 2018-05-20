package com.phylosoft.spark.learning.sql.streaming.monitoring

import com.phylosoft.spark.learning.SparkSessionConfiguration

trait Monitoring {

  this: SparkSessionConfiguration =>

  lazy val simpleListener = new SimpleListener

}
