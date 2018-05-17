package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld.pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, sum, when}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CommonCreditCardFraudPipeline(private val spark: SparkSession) extends CreditCardFraudPipeline {

  override def train(): Unit = {

    val data = spark.read.parquet("data/credit-card-fraud/data")
    data.printSchema()

    val Array(train, test) = data.randomSplit(weights = Array(.8, .2))

    val oneHot = new OneHotEncoderEstimator()
      .setInputCols(Array("amountRange"))
      .setOutputCols(Array("amountVect"))

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("amountVect", "pcaVector"))
      .setOutputCol("features")

    val estimator = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val vectorSizeHint = new VectorSizeHint()
      .setInputCol("pcaVector")
      .setSize(28)

    val pipeline = new Pipeline()
      .setStages(Array(oneHot, vectorSizeHint, vectorAssembler, estimator))

    val pipelineModel = pipeline.fit(train)

    saveModel(pipelineModel, modelPath)

    // Creating of test data
    test.repartition(20).write
      .mode("overwrite")
      .parquet(testDataPath)

  }

  override def predict(): Unit = {

    import spark.implicits._

    val schema = new StructType()
      .add(StructField("time", IntegerType))
      .add(StructField("amountRange", IntegerType))
      .add(StructField("label", IntegerType))
      .add(StructField("pcaVector", VectorType))

    val streamingData = spark.readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(testDataPath)

    val streamingRates = loadPipelineModel(modelPath)
      .transform(streamingData)
      .groupBy('label)
      .agg(
        (sum(when('prediction === 'label, 1)) / count('label)).alias("true prediction rate"),
        count('label).alias("count")
      )

    val query = streamingRates.writeStream
      .format("console")
      .outputMode("complete")
      //          .trigger(Trigger.Once())
      .start()

    query.awaitTermination()


  }

  private def testDataPath = {
    getPath("credit-card-frauld-test-data")
  }

  private def modelPath: String = {
    getPath("data/credit-card-fraud/model/first")
  }

}
