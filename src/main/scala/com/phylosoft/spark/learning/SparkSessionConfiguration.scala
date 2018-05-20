package com.phylosoft.spark.learning

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionConfiguration {

  val settings: Traversable[(String, String)]

  private val warehouseLocation = "file:///" + new File("spark-warehouse").getAbsolutePath.toString

  private lazy val conf = new SparkConf()
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.sql.session.timeZone", "UTC")
    .set("spark.sql.shuffle.partitions", "4") // keep the size of shuffles small
    .set("spark.sql.cbo.enabled", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer", "24")
    .setAll(settings)

  implicit lazy val spark: SparkSession = SparkSession.builder
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

}
