package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.services

import com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.pipeline.CommonCreditCardFraudPipeline
import org.apache.spark.sql.SparkSession

class CreditCardFraudService(private val spark: SparkSession) {

  private val pipeline = new CommonCreditCardFraudPipeline(spark)

  def train(): Unit = {
    pipeline.train()
  }

  def predict(): Unit = {
    pipeline.predict()
  }

}
