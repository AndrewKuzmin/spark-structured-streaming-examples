package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld

import com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.services.CreditCardFraudService
import org.apache.log4j.{Level, LogManager}

object CreditCardFraudTrainApp extends App with SparkSessionConfiguration {

  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)

  val settings = Map("spark.app.name" -> "CreditCardFraudTrainApp")

  val service = new CreditCardFraudService(getSparkSession(settings))
  service.train()

}
