package com.phylosoft.spark.learning.ml.streaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
  * Databricks's "MLlib Pipelines and Structured Streaming" notebook
  * https://docs.databricks.com/spark/latest/mllib/mllib-pipelines-and-stuctured-streaming.html
  */
object CreditCardFraudApp {

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    //        Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("CreditCardFraudApp")
      .set("spark.sql.session.timeZone", "UTC")
    //      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      sparkConf.set("spark.kryoserializer.buffer", "24")
    //      .set("spark.sql.shuffle.partitions", "1")
    //      .set("spark.sql.cbo.enabled", "true")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    val app = new CreditCardFraudApp(spark)
    app.start()
  }

}

class CreditCardFraudApp(private val spark: SparkSession) {

  import spark.implicits._

  def start(): Unit = {

    val data = spark.read.parquet("data/credit-card-fraud/data")
    data.printSchema()

    val Array(train, test) = data.randomSplit(weights = Array(.8, .2))

    val pipelineModel = getPipelineModel(train)

    test.repartition(20).write
      .mode("overwrite")
      .parquet(testDataPath)

    val schema = new StructType()
      .add(StructField("time", IntegerType))
      .add(StructField("amountRange", IntegerType))
      .add(StructField("label", IntegerType))
      .add(StructField("pcaVector", VectorType))

    val streamingData = spark.readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(testDataPath)

    val streamingRates = pipelineModel
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

  private def getPipelineModel(train: Dataset[Row]): PipelineModel = {

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

    pipeline.fit(train)

  }

  private def testDataPath: String = {
    import java.io.File
    val testDataPath = "file:///" + new File("credit-card-frauld-test-data").getAbsolutePath.toString
    testDataPath
  }

}
