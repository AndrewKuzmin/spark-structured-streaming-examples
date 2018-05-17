package com.phylosoft.spark.learning.ml.streaming.creditcardfrauld

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionConfiguration {

  def getSparkSession(settings: Traversable[(String, String)]): SparkSession = {

    val conf = new SparkConf()
      .setAll(settings)
      .set("spark.sql.session.timeZone", "UTC")
    //      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      sparkConf.set("spark.kryoserializer.buffer", "24")
    //      .set("spark.sql.shuffle.partitions", "1")
    //      .set("spark.sql.cbo.enabled", "true")

    SparkSession.builder
      .config(conf)
      .getOrCreate()

  }

}
