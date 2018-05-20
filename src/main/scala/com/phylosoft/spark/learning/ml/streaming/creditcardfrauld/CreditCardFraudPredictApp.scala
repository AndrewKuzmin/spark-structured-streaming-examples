package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld

import com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.services.CreditCardFraudService
import com.phylosoft.spark.learning.{Logger, SparkSessionConfiguration}

object CreditCardFraudPredictApp
  extends App
    with SparkSessionConfiguration
    with Logger {

  val settings = Map("spark.app.name" -> "CreditCardFraudPredictApp")

  val service = new CreditCardFraudService(spark)
  service.predict()

}
