package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld

import com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.services.CreditCardFraudService
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}

object CreditCardFraudTrainApp
  extends App
    with SparkSessionConfiguration
    with Logger {

  val settings = Map("spark.app.name" -> "CreditCardFraudTrainApp")

  val service = new CreditCardFraudService(spark)
  service.train()

}
