name := "SparkStructuredStreamingExamples"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"
val slf4jVersion = "1.7.16"
val log4jVersion = "1.2.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.0.5",

  "com.typesafe" % "config" % "1.3.1",
  "com.github.scopt" %% "scopt" % "3.6.0"

)
