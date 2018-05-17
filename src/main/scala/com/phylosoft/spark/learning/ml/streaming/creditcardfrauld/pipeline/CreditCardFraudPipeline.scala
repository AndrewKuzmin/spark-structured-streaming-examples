package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util.MLWritable

trait CreditCardFraudPipeline {

  def train()

  def predict()

  def saveModel[T <: MLWritable](model: T, modelOutputDirectory: String): Unit = {
    model.write.overwrite().save(modelOutputDirectory)
  }

  def loadPipelineModel(modelDirectory: String): PipelineModel = {
    val model = PipelineModel.load(modelDirectory)
    model
  }

  def getPath(pathname: String): String = {
    import java.io.File
    val path = "file:///" + new File(pathname).getAbsolutePath.toString
    path
  }

}
